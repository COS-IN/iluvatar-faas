use crate::services::invocation::dispatching::queueing_dispatcher::DispatchPolicy;
use crate::services::invocation::dispatching::EnqueueingPolicy;
use crate::services::invocation::queueing::{DeviceQueue, EnqueuedInvocation};
use crate::services::registration::RegisteredFunction;
use crate::worker_api::config::InvocationConfig;
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

//const CACHE_LOG: &str = "Landlord cache";
//const INSERT_LOG: &str = "Adding function to cache";

#[derive(Debug, Deserialize)]
pub struct LandlordConfig {
    pub cache_size: u32, // Max number of functions we are admitting, regardless of size
    // TODO: change this to max_res here and for the ansible scripts .. 
    //pub max_size: f64, // Max actual size of cache considering function footprints (exec times)
    pub log_cache_info: bool,
}

pub fn get_landlord(
    pol: EnqueueingPolicy,
    cmap: &Arc<CharacteristicsMap>,
    invocation_config: &Arc<InvocationConfig>,
    cpu_queue: &Arc<dyn DeviceQueue>,
    gpu_queue: &Option<Arc<dyn DeviceQueue>>,
) -> Result<Box<dyn DispatchPolicy>> {
    match pol {
	EnqueueingPolicy::Landlord =>  LandlordPerFuncRent::boxed(cmap, &invocation_config.landlord_config, cpu_queue, gpu_queue),
        EnqueueingPolicy::LandlordEstTime => {
	    LandlordPerFuncRent::boxed(cmap, &invocation_config.landlord_config, cpu_queue, gpu_queue)
        },
        EnqueueingPolicy::LandlordPerFuncRent => {
            LandlordPerFuncRent::boxed(cmap, &invocation_config.landlord_config, cpu_queue, gpu_queue)
        },
        EnqueueingPolicy::LandlordPerFuncRentHistorical => {
	    LandlordPerFuncRent::boxed(cmap, &invocation_config.landlord_config, cpu_queue, gpu_queue)
        },
        // landlord policy not being used, give dummy basic policy
        _ => {
	    LandlordPerFuncRent::boxed(cmap, &invocation_config.landlord_config, cpu_queue, gpu_queue)
	},
    }
}

/// Only expected to run with MQFQ for GPU queuing.
pub struct LandlordPerFuncRent {
    /// Map of FQDN -> credit
    credits: HashMap<String, f64>,
    cmap: Arc<CharacteristicsMap>,
    cfg: Arc<LandlordConfig>,
    _cpu_queue: Arc<dyn DeviceQueue>,
    gpu_queue: Arc<dyn DeviceQueue>,
    hits: u32,
    evictions: u32,
    misses: u32,
    insertions: u32,
    szhits: f64, //total size of hits
    szmisses: f64, //opportunity cost of misses
    // Keep a record of landlord operations? <(Insert/hit/miss/evict fqdn)>
}

impl LandlordPerFuncRent {
    pub fn boxed(
        cmap: &Arc<CharacteristicsMap>,
        cfg: &Option<Arc<LandlordConfig>>,
        cpu_queue: &Arc<dyn DeviceQueue>,
        gpu_queue: &Option<Arc<dyn DeviceQueue>>,
    ) -> Result<Box<dyn DispatchPolicy>> {
        match cfg {
            None => anyhow::bail!("LandlordConfig was empty"),
            Some(c) => Ok(Box::new(Self {
                credits: HashMap::new(),
                cmap: cmap.clone(),
                cfg: c.clone(),
                _cpu_queue: cpu_queue.clone(),
                gpu_queue: gpu_queue
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("GPU queue was empty trying to create LandlordPerFuncRent"))?,
		hits:0,
		evictions:0,
		misses:0,
		insertions:0,
		szhits:0.0,
		szmisses:0.0,
            })),
        }
    }

    #[inline(always)]
    fn landlog(&self, evmsg:&str) {
	if self.cfg.log_cache_info {
	    info!(cache=?self.credits, len=%self.current_occupancy(), sz=%self.current_sz_occup(), gpu_load=%self.gpu_load(), hits=%self.hits, misses=%self.misses, insertions=%self.insertions, evictions=%self.evictions, szhits=%self.szhits, szmisses=%self.szmisses, "{}", evmsg); 
	}
    }

    /// Number of resident functions.
    fn current_occupancy(&self) -> usize {
        self.credits.len()
    }

    /// Sum of the gpu exec times of resident functions 
    fn current_sz_occup(&self) -> f64 {
	let sizes:Vec<_> = self.credits.keys().map(|fqdn| self.cmap.get_gpu_exec_time(fqdn)).collect() ;
	let total_sz = sizes.iter().sum() ;
	total_sz 
    }

    /// Admit and insert New function 
    fn admit(&mut self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) {
	let fqdn = reg.fqdn.clone() ; 
        match self.credits.get_mut(&fqdn) {
            None => {
		// Most likely this is a new function, so avg e2e times will have low confidence?
		// other place we are using opp_cost which uses a better estimator.
		let cost = self.opp_cost(reg, _tid) ;
                //let cost = self.cmap.avg_gpu_e2e_t(fqdn) - self.cmap.avg_cpu_e2e_t(fqdn);
                self.credits.insert(fqdn.to_string(), cost);
            },
            Some(c) => {
		// This should not be happening? 
                *c += self.cmap.get_gpu_exec_time(&fqdn);
            },
        }
    }

    /// Get the current load on the GPU. total execution time pending
    #[allow(dead_code)] 
    fn gpu_load(&self) -> f64 {
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
	    return total_load
	}
	return 0.0 
    }
    
    /// Rent will be charged to all functions based on this 'missing' function's opportunity cost 
    fn charge_rents(&mut self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) {
	let total_rent_due = self.opp_cost(reg, tid);
	// opp cost can be negative! Dont charge rent in that case
	if total_rent_due < 0.0 {
	    info!("Not charging negative rent");
	    return 
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
	    
	    let frac_rent = total_rent_due / total_load ; 
	    
            let _ = vals
                .into_iter()
                .map(|(fqdn, len, exec)| self.credits.get_mut(&fqdn).map(|x| *x -= len * exec * frac_rent));

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
	let mqfq_est = self.gpu_queue.est_completion_time(reg, tid) ;
	let (gpu_est, est_err) = self.cmap.get_gpu_est(&reg.fqdn, mqfq_est) ;

	let cpu_est = self._cpu_queue.est_completion_time(reg, tid) ;
	
        let diff = cpu_est - gpu_est ;

	info!(tid=%tid, fqdn=%&reg.fqdn, mqfq_est=%mqfq_est, gpu_est=%gpu_est, gpu_est_err=%est_err, cpu_est=%cpu_est, 
	      "Landlord Credit");
	
	diff 
    }

    /// Add new credit. If negative, return new credit instead of accumulating 
    fn calc_add_credit(&mut self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> f64 {
        let add_credit = self.opp_cost(reg, tid);
        if add_credit < 0.0 {
            // no benefit! run on CPU
            return add_credit ; 
        }
	// self.cmap.avg_cpu_e2e_t(&reg.fqdn) - self.cmap.avg_gpu_e2e_t(&reg.fqdn);
        if let Some(credit) = self.credits.get_mut(&reg.fqdn) {
            *credit += add_credit;
	    return credit.clone()  
        }
	return 0.0 
    }

    /// if flowQ is empty, then remove from our cache, because it was marked for eviction?
    /// Other option is to add a condition that their credits must be negative to be evicted. 
    fn sync_with_mqfq(&mut self) {
	info!(cache=?self.credits, "{}", "Before Landlord Sync");
	
        if let Some(mqfq) = self.gpu_queue.expose_mqfq() {
            let mut flowqs = self
                .credits
                .keys()
                .map(|fqdn| {
                    (
                        fqdn.clone(),
                        mqfq.get(fqdn).map_or(0, |q| q.queue.len())
                    )
                })
                .collect::<Vec<(String, usize)>>();
	    
	    flowqs.retain(|(_, c)| *c == 0);
	    
	    info!(empty=%flowqs.len(), "Removing empty flowqs from gpu cache");
	    // This seems like important to tune.
	    // If rents are not enough then functions can get "stuck" in the cache if they have enough credits
	    // Agressive option is to ignore credit, which has too much churn.
	    // Maybe compromise is to give the functions a penalty in credits? 
	    for (fqdn, _c) in &flowqs{
		if let Some(cr) = self.credits.get_mut(fqdn) {
		    if *cr < 0.0 {
			info!(fqdn=%fqdn , "Removing from gpu cache, empty FlowQ"); 
			self.credits.remove(fqdn);
			self.evictions += 1; 
		    }
		    // Function has positive credit, but should be penalized somehow for having empty queue 
		    else {
			
			let penalty = self.cmap.get_gpu_exec_time(fqdn) ;
			info!(fqdn=%fqdn, penalty=%penalty, orig_cred=%cr, "Penalizing function"); 
			*cr -= penalty ;
		    }
		}
	    }
	}
    }
    
    /// Space in the GPU cache.
    /// TODO: Update using function sizes and gpu load 
    fn accepting_new(&mut self) -> bool {
	// Check here if some functions have been 'evicted'?
	self.sync_with_mqfq();

	// auto-scaling comes here? Do we want to expand?
	// factors to consider:
	// gpu_load (often zero due to small functions)
	// recent GPU tput?
	// 
	self.current_occupancy() < self.cfg.cache_size as usize 
    }

}

impl DispatchPolicy for LandlordPerFuncRent {
    
    /// Main entry point and landlord caching logic
    fn choose(&mut self, item: &Arc<EnqueuedInvocation>, tid: &TransactionId) -> Compute {

        if self.present(&item.registration.fqdn) {
	    let new_credit = self.calc_add_credit(&item.registration, tid) ;
	    if new_credit < 0.0 {
		// this function is marked for eviction
		self.misses += 1 ; 
		info!(tid=%tid, fqdn=%&item.registration.fqdn, "Negative Credit Miss");
		return Compute::CPU 
	    }
	    
	    self.hits += 1 ;
	    self.szhits += self.cmap.get_gpu_exec_time(&item.registration.fqdn);
	    // We've seen this function before so its size is more likely to be accurate 
	    info!(tid=%tid, fqdn=%&item.registration.fqdn, "Cache Hit");
	    self.landlog("Post Hit");

	    return Compute::GPU;
        }

	let potential_credits = self.opp_cost(&item.registration, tid) ;

	if self.accepting_new() && potential_credits > 0.0 {
            self.admit(&item.registration, tid);
	    self.insertions += 1 ;
	    info!(tid=%tid, fqdn=%&item.registration.fqdn, "Cache Insertion");
            return Compute::GPU;
	}

	// If we are here, either we are full, or have space but function doesnt have enough credits, so that is a miss. 
	self.misses += 1 ;
	info!(tid=%tid, fqdn=%&item.registration.fqdn, pot_creds=%potential_credits, "Cache Miss");
        self.charge_rents(&item.registration, tid);
	// add item to the ghost cache and give it some credit? 
        Compute::CPU
    }
 
}

//////////////////////////////////

