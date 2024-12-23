use crate::services::invocation::dispatching::queueing_dispatcher::DispatchPolicy;
use crate::services::invocation::dispatching::EnqueueingPolicy;
use crate::services::invocation::queueing::{DeviceQueue, EnqueuedInvocation};
use crate::services::registration::RegisteredFunction;
use crate::worker_api::config::InvocationConfig;
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::clock::{get_global_clock, Clock};
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::info;
use rand::Rng; 

#[derive(Debug, Deserialize)]
pub struct LandlordConfig {
    pub cache_size: u32, // Max number of functions we are admitting, regardless of size
    // TODO: change this to max_res here and for the ansible scripts ..
    pub max_size: f64, // Max actual size of cache considering function footprints (exec times)
    pub load_thresh: f64, // fraction for the admission control 
    pub log_cache_info: bool,
    // mode: fixed, autoscaling
}

pub fn get_landlord(
    pol: EnqueueingPolicy,
    cmap: &Arc<CharacteristicsMap>,
    invocation_config: &Arc<InvocationConfig>,
    cpu_queue: &Arc<dyn DeviceQueue>,
    gpu_queue: &Option<Arc<dyn DeviceQueue>>,
) -> Result<Box<dyn DispatchPolicy>> {
    match pol {
        EnqueueingPolicy::Landlord => {
            Landlord::boxed(cmap, &invocation_config.landlord_config, cpu_queue, gpu_queue, "LL")
        },
        EnqueueingPolicy::LRU => Landlord::boxed(cmap, &invocation_config.landlord_config, cpu_queue, gpu_queue, "LRU"),
        EnqueueingPolicy::LFU => Landlord::boxed(cmap, &invocation_config.landlord_config, cpu_queue, gpu_queue, "LFU"),
        EnqueueingPolicy::LandlordFixed => {
            Landlord::boxed(cmap, &invocation_config.landlord_config, cpu_queue, gpu_queue, "LLF")
        },
        // landlord policy not being used, give dummy basic policy
        _ => Landlord::boxed(cmap, &invocation_config.landlord_config, cpu_queue, gpu_queue, "LL"),
    }
}

/// Only expected to run with MQFQ for GPU queuing.
pub struct Landlord {
    /// Map of FQDN -> credit
    credits: HashMap<String, f64>,
    cmap: Arc<CharacteristicsMap>,
    cfg: Arc<LandlordConfig>,
    cpu_queue: Arc<dyn DeviceQueue>,
    gpu_queue: Arc<dyn DeviceQueue>,
    cachepol: String,
    residents: HashMap<String, (u32, OffsetDateTime)>, // fqdn -> invoc counts, last access. Do we keep evicted as well?
    nonresidents: HashMap<String, (u32, OffsetDateTime)>, // functions we let go
    lostcredits: HashMap<String, f64>,
    clock: Clock,
    hits: u32,
    evictions: u32,
    misses: u32,
    insertions: u32,
    /// Total size of hits
    szhits: f64,
    /// Opportunity cost of misses
    szmisses: f64,
    // Reason for misses
    negcredits: u32,
    capacitymiss: u32,
}

impl Landlord {
    pub fn boxed(
        cmap: &Arc<CharacteristicsMap>,
        cfg: &Option<Arc<LandlordConfig>>,
        cpu_queue: &Arc<dyn DeviceQueue>,
        gpu_queue: &Option<Arc<dyn DeviceQueue>>,
        cachepol: &str,
    ) -> Result<Box<dyn DispatchPolicy>> {
        match cfg {
            None => anyhow::bail!("LandlordConfig was empty"),
            Some(c) => Ok(Box::new(Self {
                credits: HashMap::new(),
                residents: HashMap::new(),
                cachepol: cachepol.to_string(),
                nonresidents: HashMap::new(),
                lostcredits: HashMap::new(),
                clock: get_global_clock(&"clock".to_string())?,
                cmap: cmap.clone(),
                cfg: c.clone(),
                cpu_queue: cpu_queue.clone(),
                gpu_queue: gpu_queue
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("GPU queue was empty trying to create LandlordPerFuncRent"))?,
                hits: 0,
                evictions: 0,
                misses: 0,
                insertions: 0,
                szhits: 0.0,
                szmisses: 0.0,
                negcredits: 0,
                capacitymiss: 0,
            })),
        }
    }

    #[inline(always)]
    fn landlog(&self, evmsg: &str) {
        if self.cfg.log_cache_info {
            info!(cache=?self.credits, len=%self.current_occupancy(), sz=%self.current_sz_occup(), gpu_load=%self.gpu_load(),
		  residents=?self.residents, nonresidents=?self.nonresidents, hits=%self.hits, misses=%self.misses,
		  insertions=%self.insertions, evictions=%self.evictions, negcredits=%self.negcredits, capacitymiss=%self.capacitymiss,
		  szhits=%self.szhits, szmisses=%self.szmisses, "{}", evmsg);
        }
    }

    /// Number of resident functions.
    fn current_occupancy(&self) -> usize {
        self.credits.len()
    }

    /// Sum of the gpu exec times of resident functions
    fn current_sz_occup(&self) -> f64 {
        self.credits.keys().map(|fqdn| self.cmap.get_gpu_exec_time(fqdn)).sum()
    }

    fn update_nonres(&mut self, fqdn: &str) {
        let tnow = self.clock.now();
        match self.nonresidents.get_mut(fqdn) {
            None => {
                self.nonresidents.insert(fqdn.to_string(), (1, tnow));
            },
            Some((c, t)) => {
                *c += 1;
                *t = tnow;
            },
        }
    }

    fn update_res(&mut self, fqdn: &str) {
        let tnow = self.clock.now();
        match self.residents.get_mut(fqdn) {
            None => {
                self.residents.insert(fqdn.to_string(), (1, tnow));
            },
            Some((c, t)) => {
                *c += 1;
                *t = tnow;
            },
        }
    }

    /// Allow functions which havent used the GPU ever
    #[allow(dead_code)]
    fn new_func_bonus(&self, fqdn: &str) -> bool {
        match self.residents.get(fqdn) {
            None => true,
            Some((_c, _t)) => false,
        }
    }

    /// Admit and insert New function
    fn admit(&mut self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) {
        match self.credits.get_mut(&reg.fqdn) {
            None => {
                // Most likely this is a new function, so avg e2e times will have low confidence?
                // other place we are using opp_cost which uses a better estimator.
                let cost = self.opp_cost(reg, tid);
                //let cost = self.cmap.avg_gpu_e2e_t(fqdn) - self.cmap.avg_cpu_e2e_t(fqdn);
                self.credits.insert(reg.fqdn.to_string(), cost);
            },
            Some(c) => {
                // This should not be happening?
                *c += self.cmap.get_gpu_exec_time(&reg.fqdn);
            },
        }
        self.insertions += 1;
	self.szhits += self.cmap.get_gpu_exec_time(&reg.fqdn);	
        info!(fqdn=%&reg.fqdn, "Cache Insertion");
        self.update_res(&reg.fqdn);
        self.lostcredits.remove(&reg.fqdn);
    }

    // We have flowqinfo.inflight which counts number of functions but not their exec time
    // Average exec time of the cache ? 
    fn get_mqfq_active_load(&self) -> f64 {
	//let running = self.gpu_queue.running();
	if let Some(flow_report) = self.gpu_queue.expose_flow_report() {
	    let total_active = flow_report.active_load.clone();
	    return total_active 
	}
	0.0
    }
    
    /// Get the current load on the GPU. total execution time pending
    fn gpu_load(&self) -> f64 {
	
        if let Some(mqfq) = self.gpu_queue.expose_mqfq() {
            let enqueued_load = self.credits.keys().fold(0.0, |acc, fqdn| {
                acc + (mqfq.get(fqdn).map_or(0.0, |q| q.queue.len() as f64) * self.cmap.get_gpu_exec_time(fqdn))
            });
	    return enqueued_load + self.get_mqfq_active_load();
	    // multiply this by the average cache exec time? We can know the active functions though? 
        }
        0.0
	//Often 0 and too low, doesnt capture functions executing on GPU (queue may be empty).
    }

    fn charge_rents(&mut self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) {
        match self.cachepol.as_str() {
            "LRU" => self.charge_rent_constant(),
            "LFU" => self.charge_rent_constant(),
            _ => self.charge_rents_ll(reg, tid),
        }
    }

    /// For LRU and LFU, always decrement by one for all
    fn charge_rent_constant(&mut self) {
        for value in self.credits.values_mut() {
            *value -= -1.0
        }
    }

    /// Rent will be charged to all functions based on this 'missing' function's opportunity cost
    fn charge_rents_ll(&mut self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) {
        let total_rent_due = self.opp_cost(reg, tid);
        // opp cost can be negative! Don't charge rent in that case
        if total_rent_due < 0.0 {
            info!("Not charging negative rent");
            return;
        }
        // rent is charged proportional to n*exec from each fqdn

        if let Some(mqfq) = self.gpu_queue.expose_mqfq() {
            let vals = self
                .credits
                .keys()
                .map(|fqdn| {
                    (
                        fqdn.clone(),
                        mqfq.get(fqdn).map_or(0.0, |q| q.queue.len() as f64),
                        self.cmap.get_gpu_exec_time(fqdn),
                    )
                })
                .collect::<Vec<(String, f64, f64)>>();
            let total_load = vals.iter().fold(0.0, |acc, (_, x, y)| acc + (x * y));

            info!(total_load=%total_load, "GPU Load");

            let frac_rent = total_rent_due / total_load;

            vals.into_iter().for_each(|(fqdn, len, exec)| {
                if let Some(x) = self.credits.get_mut(&fqdn) {
                    *x -= len * exec * frac_rent
                };
            });

            // This might result in some getting evicted. We should know about these?
            self.credits.retain(|_fqdn, c| *c > 0.0); // we still see functions with negative credit?

            self.landlog("Post Rent");
        }
    }

    /// Check if locally present. See if credits ok or marked for eviction?
    fn present(&self, fqdn: &str) -> bool {
        // See if credits are positive?
        self.credits.contains_key(fqdn)
    }

    /// The new credit is equal to the difference between the CPU and GPU execution times
    /// Equivalent to the opportunity cost of (not) caching the item
    fn opp_cost(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> f64 {
        let mqfq_est = self.gpu_queue.est_completion_time(reg, tid);
        let (gpu_est, est_err) = self.cmap.get_gpu_est(&reg.fqdn, mqfq_est);
        let cpu_est = self.cpu_queue.est_completion_time(reg, tid);
        info!(tid=%tid, fqdn=%&reg.fqdn, mqfq_est=%mqfq_est, gpu_est=%gpu_est, gpu_est_err=%est_err, cpu_est=%cpu_est, "Landlord Credit");

        match self.cachepol.as_str() {
            "LFU" => 1.0,
            "LRU" => match self.present(&reg.fqdn) {
                true => 0.0, // LRU no extra credit, max is 1
                false => 1.0,
            },
            _ => cpu_est - gpu_est,
        }
    }

    /// Add new credit. If negative, return new credit instead of accumulating
    fn calc_add_credit(&mut self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> f64 {
        let add_credit = self.opp_cost(reg, tid);
        if add_credit < 0.0 {
            // no benefit! run on CPU
            return add_credit;
        }
        match self.credits.get_mut(&reg.fqdn) {
            Some(credit) => {
                *credit += add_credit;
                *credit
            },
            None => 0.0,
        }
    }

    /// if flowQ is empty, then remove from our cache, because it was marked for eviction?
    /// Other option is to add a condition that their credits must be negative to be evicted.
    #[allow(dead_code)]
    fn sync_with_mqfq(&mut self) {
        info!(cache=?self.credits, "{}", "Before Landlord Sync");
        if let Some(mqfq) = self.gpu_queue.expose_mqfq() {
            self.credits.retain(|fqdn, cr| {
                match mqfq.get(fqdn) {
                    Some(q) if q.queue.is_empty() => {
                        if *cr < 0.0 {
                            info!(fqdn=%fqdn, "Removing from gpu cache, empty FlowQ");
                            self.evictions += 1;
                            // Do we update residents here? NO. lets keep historical residence info
                            // Function may have been 'temporarily' evicted. More useful for determining novelty, GPU hit % per func, etc.
                            // Thus, a function can be in BOTH resident and non-resident sets
                            false
                        } else {
                            // Function has positive credit, but should be penalized somehow for having empty queue
                            let penalty = self.cmap.get_gpu_exec_time(fqdn);
                            info!(fqdn=%fqdn, penalty=%penalty, orig_cred=%cr, "Penalizing function");
                            *cr -= penalty;
                            *cr > 0.0
                        }
                    },
                    _ => false,
                }
            })
        }
    }

    /// Evict the function from GPU credits
    fn evict_victim(&mut self, fqdn: &str) {
        self.credits.remove(fqdn);
        self.evictions += 1;
        self.landlog("Eviction");
    }

    fn potential_victims(&self) -> Vec<(String, usize)> {
        match self.gpu_queue.expose_mqfq() {
            None => vec![],
            Some(mqfq) => self
                .credits
                .keys()
                .map(|fqdn| (fqdn.clone(), mqfq.get(fqdn).map_or(0, |q| q.queue.len())))
                .filter(|(_, l)| *l == 0)
                .collect::<Vec<(String, usize)>>(),
        }
    }

    /// Main eviction routine. Find a victim whose remaining credits are lower than offered.
    fn try_find_victim(&mut self, acc_pot_credits: f64) -> bool {
        let pot_victims = self.potential_victims();
        if pot_victims.is_empty() {
            return false;
        }
        info!(empty=%pot_victims.len(), other=%acc_pot_credits, "Trying to find victim");
        match pot_victims
            .into_iter()
            .map(|(f, _c)| {
                (
                    self.credits
                        .get(&f)
                        .map_or(OrderedFloat(f64::MIN), |c| OrderedFloat(*c)),
                    f,
                )
            })
            .min_by(|a, b| a.0.cmp(&b.0))
        {
            None => false,
            Some(victim) => {
                info!(victim=%victim.1, counter=%acc_pot_credits, "Victim vs counter offer");
                let deficit = victim.0 - acc_pot_credits;
                if deficit.0 > 0.0 {
                    // victim has more credits,
                    false
                } else {
                    // qs is do we evict it here? or somewhere else?
                    self.evict_victim(&victim.1);
                    true
                }
            },
        }
    }

    /// Will always evict item with smallest access time
    fn try_lru_evict(&mut self) -> bool {
        let pot_victims = self.potential_victims();
        if pot_victims.is_empty() {
            return false;
        }
        info!(empty=%pot_victims.len(), "Trying to find victim");

        match pot_victims
            .into_iter()
            .map(|(f, _t)| (self.residents.get(&f).unwrap_or(&(0, OffsetDateTime::UNIX_EPOCH)).1, f))
            .min_by(|a, b| a.0.cmp(&b.0))
        {
            None => false,
            Some(victim) => {
                self.evict_victim(&victim.1);
                true
            },
        }
    }

    /// Least Frequently Used Eviction
    fn try_lfu_evict(&mut self) -> bool {
        let pot_victims = self.potential_victims();
        if pot_victims.is_empty() {
            return false;
        }
        info!(empty=%pot_victims.len(), "Trying to find victim");

        match pot_victims
            .into_iter()
            .map(|(f, _t)| (self.residents.get(&f).unwrap_or(&(0, OffsetDateTime::UNIX_EPOCH)).0, f))
            .min_by(|a, b| a.0.cmp(&b.0))
        {
            None => false,
            Some(victim) => {
                self.evict_victim(&victim.1);
                true
            },
        }
    }

    /// For functions not in cache, accumulate how much credit they already have.
    fn accum_potential_credits(&mut self, fqdn: &str, newc: f64) -> f64 {
        match self.lostcredits.get_mut(fqdn) {
            Some(c) => {
                *c += newc;
                *c
            },
            _ => {
                self.lostcredits.insert(fqdn.to_string(), newc);
                newc
            },
        }
    }

    /// Soft expansion is when we have empty MQFQ queues so can admit a new function, but strictly are over limit
    #[allow(dead_code)]
    fn soft_expand(&mut self) -> bool {
        let remaining = self.cfg.cache_size as i32 - self.current_occupancy() as i32;

        if remaining > 0 {
            return true;
        }
        // Expand for LRU etc?
        if self.cachepol != "LL" {
            return false;
        }
        let expected_sz = self.current_sz_occup();
        let real_sz = self.gpu_load();

        if real_sz < 0.9 * expected_sz {
            // we can let /some/ functions in, but not too many, in case huge bursty arrivals from everyone?
            // TODO: Add limits to soft-ex.
            // accounting will be tricky
            info!(expected_sz=%expected_sz, real_sz=%real_sz, "Soft expanding");
            return true; //YOLO
        }
        false
    }

    /// Active functions footprint smaller than capacity
    #[allow(dead_code)]
    #[inline(always)]
    fn accepting_new(&self) -> bool {
        //let remaining_slots = self.cfg.cache_size as i32 - self.current_occupancy() as i32 ;
	let remaining_sz = self.cfg.max_size - self.current_sz_occup() ; 
	return remaining_sz > 0.0	    
    }

    /// Based on eviction policy, return true if managed to evict victim
    fn try_evict_pol(&mut self, acc_pot_credits: f64) -> bool {
        let victim_found = match self.cachepol.as_str() {
            "LRU" => self.try_lru_evict(),
            "LFU" => self.try_lfu_evict(),
            _ => self.try_find_victim(acc_pot_credits),
        };
	return victim_found 
    }
    
    /// Main admission control. We may have space, but should be admit? Depends on load, other heuristics
    fn admit_filter(&mut self, item: &Arc<EnqueuedInvocation>, tid: &TransactionId) -> bool {
        let potential_credits = self.opp_cost(&item.registration, tid);
	// get the number of gpu invokes for this function? 
        let acc_pot_credits = self.accum_potential_credits(&item.registration.fqdn, potential_credits);
	let (n_gpu, _) = self.residents.get(&item.registration.fqdn).unwrap_or(&(0, OffsetDateTime::UNIX_EPOCH)).clone();
	
	let mut _eviction_attempted  = false ;
	let mut _eviction_success = false ;
	
	// try to see if cache is full and if so, try evicting
	// if cache is full, should we always try to evict, irrespective of credits?
	// 
	if !self.accepting_new() {
	    _eviction_attempted = true;
	    _eviction_success = self.try_evict_pol(100000000000.0); 
	}
	// At this point we've made some space, and the real admission control starts, is this new fqdn worthy?
	
	// A1. New function bonus on low GPU load 
	if self.gpu_load() < 0.2 * self.cfg.max_size {
	    // this is low load. Chance = 1/1+n_gpu
	    let p_new = 1.0/(1.0+n_gpu as f64);
	    // remember this is irrespective of the potential credits 
	    let r = rand::thread_rng().gen_range(0.0..1.0);
	    if r < p_new {
		// won the lottery!
		info!("ADMISSION LOTTERY");
		return true 
	    }
	}
	
	// A2. this is the place for static criteria. Nothing yet.

	// A3. Also an underload condition but different threshold? Can be merged with A1.
	// Admit if potential credits are enough without evicting 
	
	let gpu_load_factor = self.gpu_load() / self.cfg.max_size ;
	if gpu_load_factor < self.cfg.load_thresh {
	    //  we are under-loaded, so only the potential_credits check is enough
	    info!(fqdn=%&item.registration.fqdn, gpu_load=%self.gpu_load(), "ADMIT_LOW_LOAD");
	    return potential_credits > 0.0 
	}
	// A4. if above load thresh, the bar is higher, i.e., it must compete with some inactive function and have enough to evict

	// if eviction_attempted {
	//     if eviction_success {
	// 	// victim has already been found, not need to evict again
	// 	info!("ADMIT_VICTIM");
	// 	return true 
	//     }
	//     else {
	// 	info!("DENY_VICTIM");
	// 	return false 
	//     }
	// }
//	else { // cache was not full earlier, so try here 
        let victim_found = self.try_evict_pol(acc_pot_credits); 
	if !victim_found {
	    info!("DENY_VICTIM");
	    return false 
	}
	info!("ADMIT_VICTIM");
	return true 
//	}
	
    }

}

//////////////////////////////

impl DispatchPolicy for Landlord {
    /// Main entry point and landlord caching logic
    fn choose(&mut self, item: &Arc<EnqueuedInvocation>, tid: &TransactionId) -> Compute {

        if self.present(&item.registration.fqdn) {
            // This doesnt decrease credit
            let new_credit = self.calc_add_credit(&item.registration, tid);
            let pos_credit = match self.credits.get(&item.registration.fqdn) {
                Some(cr) => *cr + new_credit > 0.0,
                _ => false,
            };
            if !pos_credit {
                // this function is marked for eviction
                // we really want to minimize this case, function is on gpu already. estimate can be wrong?
                self.misses += 1;
                self.negcredits += 1;
		self.szmisses += self.cmap.get_gpu_exec_time(&item.registration.fqdn);	
                info!(tid=%tid, fqdn=%&item.registration.fqdn,"MISS_INSUFFICIENT_CREDITS");
                self.update_nonres(&item.registration.fqdn);
                return Compute::CPU;
            }
            self.hits += 1;
            self.szhits += self.cmap.get_gpu_exec_time(&item.registration.fqdn);
            // We've seen this function before so its size is more likely to be accurate
            info!(tid=%tid, fqdn=%&item.registration.fqdn, opp_cost=%new_credit, "Cache Hit");
            self.landlog("HIT_PRESENT");
            self.update_res(&item.registration.fqdn);
            return Compute::GPU;
        }

	// not present. Either insert/admit or MISS. Charge rents in every case 
	self.charge_rents(&item.registration, tid);
		
	// now the tricky admission criteria. We may have space to insert, but should we? 
	let can_admit = self.admit_filter(item, tid); 
	
        if can_admit {
            // We found space!
            self.admit(&item.registration, tid);
            return Compute::GPU
        } else {
            // If we are here, either we are full, or have space but function doesnt have enough credits, so that is a miss.
            self.misses += 1;
            self.capacitymiss += 1;
	    self.szmisses += self.cmap.get_gpu_exec_time(&item.registration.fqdn);	
            info!(tid=%tid, fqdn=%&item.registration.fqdn, "Cache Miss Admission");
            self.update_nonres(&item.registration.fqdn.clone());
            return Compute::CPU
        }
    }
}

//////////////////////////////////
