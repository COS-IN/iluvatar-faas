use dashmap::DashMap;
use ordered_float::OrderedFloat;
use std::cmp::Ordering;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error};

#[derive(Debug)]
pub enum Values {
    Duration(Duration),
    F64(f64),
    U64(u64),
    Str(String),
}

pub fn unwrap_val_dur(value: &Values) -> Duration {
    match value {
        Values::Duration(v) => v.clone(),
        _ => {
            error!(error = "incorrect unwrap", "unwrap_val_dur not of type Duration");
            Duration::new(0, 0)
        }
    }
}

pub fn unwrap_val_f64(value: &Values) -> f64 {
    match value {
        Values::F64(v) => v.clone(),
        _ => {
            error!(error = "incorrect unwrap", "unwrap_val_f64 not of type f64");
            0.0
        }
    }
}
/// A safe comparator for f64 values
pub fn compare_f64(lhs: &f64, rhs: &f64) -> Ordering {
    let lhs: OrderedFloat<f64> = OrderedFloat(*lhs);
    let rhs: OrderedFloat<f64> = OrderedFloat(*rhs);
    lhs.cmp(&rhs)
}

pub fn unwrap_val_u64(value: &Values) -> u64 {
    match value {
        Values::U64(v) => v.clone(),
        _ => {
            error!(error = "incorrect unwrap", "unwrap_val_u64 not of type u64");
            0
        }
    }
}

pub fn unwrap_val_str(value: &Values) -> String {
    match value {
        Values::Str(v) => v.clone(),
        _ => {
            error!(error = "unwrap_val_str not of type String");
            "None".to_string()
        }
    }
}

////////////////////////////////////////////////////////////////
/// Aggregators for CharacteristicsMap
#[derive(Debug)]
pub struct AgExponential {
    alpha: f64,
}

impl AgExponential {
    pub fn new(alpha: f64) -> Self {
        AgExponential { alpha }
    }

    fn accumulate(&self, old: &f64, new: &f64) -> f64 {
        (new * self.alpha) + (old * (1.0 - self.alpha))
    }
    fn accumulate_dur(&self, old: &Duration, new: &Duration) -> Duration {
        new.mul_f64(self.alpha) + old.mul_f64(1.0 - self.alpha)
    }
}

////////////////////////////////////////////////////////////////
/// CharacteristicsMap Implementation  

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Characteristics {
    /// Running avg of _all_ times on CPU for invocations
    /// Recorded by CpuQueueingInvoker::invoke_on_container
    ExecTime,
    /// Time on CPU for a warm invocation
    /// Recorded by CpuQueueingInvoker::invoke_on_container
    WarmTime,
    /// Time on CPU for a pre-warmed invocation
    /// The container was previously started, but no invocation was run on it
    /// Recorded by CpuQueueingInvoker::invoke_on_container
    PreWarmTime,
    /// E2E time for a CPU cold start
    /// Recorded by CpuQueueingInvoker::invoke_on_container
    ColdTime,
    /// Running avg of _all_ times on GPU for invocations
    /// Recorded by GpuQueueingInvoker::invoke_on_container
    GpuExecTime,
    /// Time on GPU for a warm invocation
    /// Recorded by GpuQueueingInvoker::invoke_on_container
    GpuWarmTime,
    /// Time on GPU for a pre-warmed invocation
    /// The container was previously started, but no invocation was run on it
    /// Recorded by GpuQueueingInvoker::invoke_on_container
    GpuPreWarmTime,
    /// E2E time for a GPU cold start
    /// Recorded by GpuQueueingInvoker::invoke_on_container
    GpuColdTime,
    /// The last time an invocation happened
    /// Recorded internally by the [CharacteristicsMap::add_iat] function
    LastInvTime,
    /// The running avg IAT
    /// Recorded by iluvatar_worker_library::worker_api::ilúvatar_worker::IluvatarWorkerImpl::invoke and iluvatar_worker_library::worker_api::ilúvatar_worker::IluvatarWorkerImpl::invoke_async
    IAT,
    /// The running avg memory usage
    /// TODO: record this somewhere
    MemoryUsage,
}

#[derive(Debug)]
pub struct CharacteristicsMap {
    map: DashMap<String, DashMap<Characteristics, Values>>,
    ag: AgExponential,
}

impl CharacteristicsMap {
    pub fn new(ag: AgExponential) -> Self {
        let map = CharacteristicsMap {
            map: DashMap::new(),
            ag,
        };
        // TODO: Implement file restore functionality here

        map
    }

    pub fn add(&self, fqdn: &String, chr: Characteristics, value: Values, use_accum: bool) -> &Self {
        let e0 = self.map.get_mut(fqdn);

        match e0 {
            // dashself.map of given fqdn
            Some(v0) => {
                let e1 = v0.get_mut(&chr);
                // entry against given characteristic
                match e1 {
                    Some(mut v1) => {
                        if use_accum {
                            *v1 = match &v1.value() {
                                Values::Duration(d) => {
                                    Values::Duration(self.ag.accumulate_dur(d, &unwrap_val_dur(&value)))
                                }
                                Values::F64(f) => Values::F64(self.ag.accumulate(f, &unwrap_val_f64(&value))),
                                Values::U64(_) => todo!(),
                                Values::Str(_) => todo!(),
                            };
                        } else {
                            *v1 = match &v1.value() {
                                Values::Duration(_d) => Values::Duration(unwrap_val_dur(&value)),
                                Values::F64(_f) => Values::F64(unwrap_val_f64(&value)),
                                Values::U64(_) => todo!(),
                                Values::Str(_) => todo!(),
                            };
                        }
                    }
                    None => {
                        v0.insert(chr, value);
                    }
                }
            }
            None => {
                // dashmap for given fname does not exist create and populate
                let d = DashMap::new();
                d.insert(chr, value);
                self.map.insert(fqdn.clone(), d);
            }
        }

        self
    }

    pub fn add_iat(&self, fqdn: &String) {
        let time_now = SystemTime::now();
        let time_now = time_now.duration_since(UNIX_EPOCH).expect("Time went backwards");

        let last_inv_time = self
            .lookup(fqdn, &Characteristics::LastInvTime)
            .unwrap_or(Values::Duration(Duration::new(0, 0)));
        let last_inv_time = unwrap_val_dur(&last_inv_time);

        if last_inv_time.as_secs_f64() > 0.1 {
            let iat = time_now.as_secs_f64() - last_inv_time.as_secs_f64();
            self.add(fqdn, Characteristics::IAT, Values::F64(iat), true);
        }

        self.add(
            fqdn,
            Characteristics::LastInvTime,
            Values::Duration(time_now.clone()),
            false,
        );
    }

    pub fn lookup(&self, fqdn: &String, chr: &Characteristics) -> Option<Values> {
        let e0 = self.map.get(fqdn)?;
        let e0 = e0.value();
        let v = e0.get(chr)?;
        let v = v.value();

        Some(self.clone_value(v))
    }
    /// Returns the execution time as tracked by [Characteristics::ExecTime]
    /// Returns 0.0 if it was not found, or an error occured
    pub fn get_exec_time(&self, fqdn: &String) -> f64 {
        if let Some(exectime) = self.lookup(fqdn, &Characteristics::ExecTime) {
            unwrap_val_f64(&exectime)
        } else {
            0.0
        }
    }
    /// Returns the execution time as tracked by [Characteristics::GpuExecTime]
    /// Returns 0.0 if it was not found, or an error occured
    pub fn get_gpu_exec_time(&self, fqdn: &String) -> f64 {
        if let Some(exectime) = self.lookup(fqdn, &Characteristics::GpuExecTime) {
            unwrap_val_f64(&exectime)
        } else {
            0.0
        }
    }
    /// Returns the execution time as tracked by [Characteristics::GpuColdTime]
    /// Returns 0.0 if it was not found, or an error occured
    pub fn get_gpu_cold_time(&self, fqdn: &String) -> f64 {
        if let Some(exectime) = self.lookup(fqdn, &Characteristics::GpuColdTime) {
            unwrap_val_f64(&exectime)
        } else {
            0.0
        }
    }
    /// Returns the execution time as tracked by [Characteristics::GpuWarmTime]
    /// Returns 0.0 if it was not found, or an error occured
    pub fn get_gpu_warm_time(&self, fqdn: &String) -> f64 {
        if let Some(exectime) = self.lookup(fqdn, &Characteristics::GpuWarmTime) {
            unwrap_val_f64(&exectime)
        } else {
            0.0
        }
    }
    /// Returns the execution time as tracked by [Characteristics::GpuPreWarmTime]
    /// Returns 0.0 if it was not found, or an error occured
    pub fn get_gpu_prewarm_time(&self, fqdn: &String) -> f64 {
        if let Some(exectime) = self.lookup(fqdn, &Characteristics::GpuPreWarmTime) {
            unwrap_val_f64(&exectime)
        } else {
            0.0
        }
    }
    /// Returns the execution time as tracked by [Characteristics::WarmTime]
    /// Returns 0.0 if it was not found, or an error occured
    pub fn get_warm_time(&self, fqdn: &String) -> f64 {
        if let Some(exectime) = self.lookup(fqdn, &Characteristics::WarmTime) {
            unwrap_val_f64(&exectime)
        } else {
            0.0
        }
    }
    /// Returns the execution time as tracked by [Characteristics::PreWarmTime]
    /// Returns 0.0 if it was not found, or an error occured
    pub fn get_prewarm_time(&self, fqdn: &String) -> f64 {
        if let Some(exectime) = self.lookup(fqdn, &Characteristics::PreWarmTime) {
            unwrap_val_f64(&exectime)
        } else {
            0.0
        }
    }
    /// Returns the execution time as tracked by [Characteristics::ExecTime]
    /// Returns 0.0 if it was not found, or an error occured
    pub fn get_cold_time(&self, fqdn: &String) -> f64 {
        if let Some(exectime) = self.lookup(fqdn, &Characteristics::ColdTime) {
            unwrap_val_f64(&exectime)
        } else {
            0.0
        }
    }
    /// Returns the IAT as tracked by [Characteristics::IAT]
    /// Returns 0.0 if it was not found, or an error occured
    pub fn get_iat(&self, fqdn: &String) -> f64 {
        if let Some(iat) = self.lookup(fqdn, &Characteristics::IAT) {
            unwrap_val_f64(&iat)
        } else {
            0.0
        }
    }

    pub fn clone_value(&self, value: &Values) -> Values {
        match value {
            Values::F64(v) => Values::F64(*v),
            Values::U64(v) => Values::U64(*v),
            Values::Duration(v) => Values::Duration(v.clone()),
            Values::Str(v) => Values::Str(v.clone()),
        }
    }

    pub fn dump(&self) {
        for e0 in self.map.iter() {
            let fqdn = e0.key();
            let omap = e0.value();

            for e1 in omap.iter() {
                let chr = e1.key();
                let value = e1.value();

                debug!(component = "CharacteristicsMap", "{} -- {:?},{:?}", fqdn, chr, value);
            }
        }
    }
}

#[cfg(test)]
mod charmap {
    use super::*;

    #[test]
    fn duration() {
        // Test 4 using Duration datatype for ExecTime
        let m = CharacteristicsMap::new(AgExponential::new(0.6));
        println!("--------------------------------------------------------------------");
        println!("Test 4: Using Duration Datatype for ExecTime");

        println!("      : Adding one element");
        m.add(
            &"video_processing.0.0.1".to_string(),
            Characteristics::ExecTime,
            Values::Duration(Duration::new(2, 30)),
            true,
        );
        println!("      : looking up the new element");
        println!(
            "      :   {:?}",
            unwrap_val_dur(
                &m.lookup(&"video_processing.0.0.1".to_string(), &Characteristics::ExecTime)
                    .unwrap()
            )
        );
        println!("      : Adding three more");
        m.add(
            &"video_processing.0.0.1".to_string(),
            Characteristics::ExecTime,
            Values::Duration(Duration::new(5, 50)),
            true,
        );
        m.add(
            &"video_processing.0.0.1".to_string(),
            Characteristics::ExecTime,
            Values::Duration(Duration::new(5, 50)),
            true,
        );
        m.add(
            &"video_processing.0.0.1".to_string(),
            Characteristics::ExecTime,
            Values::Duration(Duration::new(5, 50)),
            true,
        );
        println!("      : dumping whole map");
        m.dump();
        assert_eq!(
            unwrap_val_dur(
                &m.lookup(&"video_processing.0.0.1".to_string(), &Characteristics::ExecTime)
                    .unwrap()
            ),
            Duration::from_secs_f64(4.808000049)
        );
    }

    #[test]
    fn lookup() {
        let m = CharacteristicsMap::new(AgExponential::new(0.6));

        let push_video = || {
            m.add(
                &"video_processing.0.0.1".to_string(),
                Characteristics::ExecTime,
                Values::F64(0.3),
                true,
            );
            m.add(
                &"video_processing.0.0.1".to_string(),
                Characteristics::ColdTime,
                Values::F64(0.9),
                true,
            );
            m.add(
                &"video_processing.0.0.1".to_string(),
                Characteristics::WarmTime,
                Values::F64(0.6),
                true,
            );

            m.add(
                &"video_processing.0.1.1".to_string(),
                Characteristics::ExecTime,
                Values::F64(0.4),
                true,
            );
            m.add(
                &"video_processing.0.1.1".to_string(),
                Characteristics::ColdTime,
                Values::F64(1.9),
                true,
            );
            m.add(
                &"video_processing.0.1.1".to_string(),
                Characteristics::WarmTime,
                Values::F64(1.6),
                true,
            );

            m.add(
                &"json_dump.0.1.1".to_string(),
                Characteristics::ExecTime,
                Values::F64(0.4),
                true,
            );
            m.add(
                &"json_dump.0.1.1".to_string(),
                Characteristics::ColdTime,
                Values::F64(1.9),
                true,
            );
            m.add(
                &"json_dump.0.1.1".to_string(),
                Characteristics::WarmTime,
                Values::Duration(Duration::from_secs_f64(1.6)),
                true,
            );
        };

        // Test 1 single entries
        push_video();
        println!("--------------------------------------------------------------------");
        println!("Test 1: Singular additions");
        println!(
            "      : lookup ExecTime of json - {}",
            unwrap_val_f64(
                &m.lookup(&"json_dump.0.1.1".to_string(), &Characteristics::ExecTime)
                    .unwrap()
            )
        );
        println!("      : dumping whole map");
        m.dump();
        assert_eq!(
            unwrap_val_f64(
                &m.lookup(&"json_dump.0.1.1".to_string(), &Characteristics::ExecTime)
                    .unwrap()
            ),
            0.4
        );
        assert_eq!(
            unwrap_val_dur(
                &m.lookup(&"json_dump.0.1.1".to_string(), &Characteristics::WarmTime)
                    .unwrap()
            ),
            Duration::from_secs_f64(1.6)
        );
    }

    #[test]
    fn accumulation() {
        let m = CharacteristicsMap::new(AgExponential::new(0.6));

        m.add(
            &"video_processing.0.0.1".to_string(),
            Characteristics::ExecTime,
            Values::F64(0.3),
            true,
        );
        m.add(
            &"video_processing.0.0.1".to_string(),
            Characteristics::ColdTime,
            Values::F64(0.9),
            true,
        );
        m.add(
            &"video_processing.0.0.1".to_string(),
            Characteristics::WarmTime,
            Values::F64(0.6),
            true,
        );

        m.add(
            &"video_processing.0.1.1".to_string(),
            Characteristics::ExecTime,
            Values::F64(0.4),
            true,
        );
        m.add(
            &"video_processing.0.1.1".to_string(),
            Characteristics::ColdTime,
            Values::F64(1.9),
            true,
        );
        m.add(
            &"video_processing.0.1.1".to_string(),
            Characteristics::WarmTime,
            Values::F64(1.6),
            true,
        );

        // Test 3 exponential average to accumulate
        m.add(
            &"video_processing.0.0.1".to_string(),
            Characteristics::ExecTime,
            Values::F64(0.5),
            true,
        )
        .add(
            &"video_processing.0.0.1".to_string(),
            Characteristics::ExecTime,
            Values::F64(0.5),
            true,
        )
        .add(
            &"video_processing.0.0.1".to_string(),
            Characteristics::ExecTime,
            Values::F64(0.5),
            true,
        );
        println!("--------------------------------------------------------------------");
        println!("Test 3: three additions of ExecTime 0.5 to vp.0.0.1 - should be exponential average");
        println!("      : dumping whole map");
        m.dump();
        assert_eq!(
            unwrap_val_f64(
                &m.lookup(&"video_processing.0.0.1".to_string(), &Characteristics::ExecTime)
                    .unwrap()
            ),
            0.48719999999999997
        );
    }

    #[test]
    fn iat_calcualtion() {
        use float_cmp::approx_eq;
        use std::thread::sleep;

        let m = CharacteristicsMap::new(AgExponential::new(0.6));
        let fjd_011 = "json_dump.0.1.1".to_string();

        let verify_iat_lookup = |fname: &String, val_expc: f64| {
            let val = m.lookup(fname, &Characteristics::IAT).unwrap_or(Values::F64(0.0));
            assert!(approx_eq!(f64, unwrap_val_f64(&val), val_expc, epsilon = 0.005));
            // assert_eq!( unwrap_val_f64( &val ), val_expc );
        };

        verify_iat_lookup(&fjd_011, 0.0);
        m.add_iat(&fjd_011);
        verify_iat_lookup(&fjd_011, 0.0);

        sleep(Duration::from_secs_f64(1.0));
        m.add_iat(&fjd_011);
        verify_iat_lookup(&fjd_011, 1.0);

        sleep(Duration::from_secs_f64(1.0));
        m.add_iat(&fjd_011);
        verify_iat_lookup(&fjd_011, 1.0);

        sleep(Duration::from_secs_f64(2.0));
        m.add_iat(&fjd_011);
        verify_iat_lookup(&fjd_011, 1.6); // 1.0, 1.0, 2.0 -> exp moving average should be 1.6

        /* Using Pandas
         * >>> data = [ 1.0, 1.0, 2.0 ]
         * >>> df = pd.DataFrame( data )
         * >>> df.ewm( alpha=0.6, adjust=False ).mean()
         *      0
         *      0  1.0
         *      1  1.0
         *      2  1.6
         */
    }
}
