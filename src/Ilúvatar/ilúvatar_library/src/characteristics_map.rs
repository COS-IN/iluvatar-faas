use dashmap::DashMap;
use std::time::Duration;
use tracing::debug;

#[derive(Debug)]
pub enum Values {
    Duration(Duration),
    F64(f64),
    U64(u64),
    Str(String)
}

pub fn unwrap_val_dur ( value: &Values ) -> Duration {
    match value {
        Values::Duration(v) => v.clone(), 
        _  => {
            debug!(error="incorrect unwrap","unwrap_val_dur not of type Duration");
            Duration::new(0,0)
        }
    }
}

pub fn unwrap_val_f64 ( value: &Values ) -> f64 {
    match value {
        Values::F64(v) => v.clone(), 
        _  => {
            debug!(error="incorrect unwrap","unwrap_val_f64 not of type f64");
            0.0     
        }
    }
}

pub fn unwrap_val_u64 ( value: &Values ) -> u64 {
    match value {
        Values::U64(v) => v.clone(), 
        _  => {
            debug!(error="incorrect unwrap","unwrap_val_u64 not of type u64");
            0    
        }
    }
}

pub fn unwrap_val_str ( value: &Values ) -> String {
    match value {
        Values::Str(v) => v.clone(), 
        _  => {
            debug!(error="unwrap_val_str not of type String");
            "None".to_string()   
        }
    }
}

////////////////////////////////////////////////////////////////
/// Aggregators for CharacteristicsMap 
#[derive(Debug)]
pub struct AgExponential {
    alpha: f64
}

impl AgExponential {
    pub fn new( alpha: f64 ) -> Self {
        AgExponential {
            alpha
        }
    }

    fn accumulate ( &self, old: &f64, new: &f64 ) -> f64 {
           ( new * self.alpha ) + ( old * (1.0-self.alpha) ) 
    }
}

////////////////////////////////////////////////////////////////
/// CharacteristicsMap Implementation  

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Characteristics {
    ExecTime,
    WarmTime,
    ColdTime,
    MemoryUsage
}

#[derive(Debug)]
pub struct CharacteristicsMap {
    map: DashMap<String,DashMap<Characteristics,Values>>,
    ag: AgExponential 
}

impl CharacteristicsMap {
    pub fn new( ag: AgExponential ) -> Self {
        let map = CharacteristicsMap {
            map: DashMap::new(),
            ag
        };
        // TODO: Implement file restore functionality here 
        
        map
    }

    pub fn add( &self, fname: String, chr: Characteristics, value: Values) -> &Self {
        let e0 = self.map.get_mut( &fname );

        match e0 {
            // dashself.map of given fname
            Some(v0) => {
               let e1 = v0.get_mut( &chr );
               // entry against given characteristic
               match e1 {
                   Some(mut v1) => {
                           *v1 = Values::F64( self.ag.accumulate( &unwrap_val_f64(&v1.value()), &unwrap_val_f64(&value) ));
                   },
                   None => {
                       v0.insert( chr, value );
                   }
               }
            },
            None => {
                // dashmap for given fname does not exist create and populate
                let d = DashMap::new();
                d.insert( chr, value );
                self.map.insert( fname, d );
            }
        }

        self
    }
    
    pub fn lookup (&self, fname: String, chr: Characteristics ) -> Option<Values> {
       let e0 = self.map.get( &fname )?;
       let e0 = e0.value();
       let v = e0.get( &chr )?;
       let v = v.value();

       Some( self.clone_value( v ) )
    }
    
    pub fn clone_value( &self, value: &Values ) -> Values {
        match value {
            Values::F64(v) => Values::F64(*v), 
            Values::U64(v) => Values::U64(*v), 
            Values::Duration(v) => Values::Duration(v.clone()), 
            Values::Str(v) => Values::Str(v.clone()) 
        }
    }

    pub fn dump( &self ) {
        for e0 in self.map.iter() {
            let fname = e0.key();
            let omap = e0.value();

            for e1 in omap.iter() {
                let chr = e1.key();
                let value = e1.value();
                
                debug!(component="CharacteristicsMap", "{} -- {:?},{:?}", fname, chr, value);
            }
        }
    }
}

#[cfg(test)]
mod charmap {
    use super::*;

    #[test]
    fn everything() -> Result<(), String> {
        let m = CharacteristicsMap::new( AgExponential::new( 0.6 ) );
        
        let push_video = || {
            m.add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::F64(0.3));
            m.add( "video_processing.0.0.1".to_string(), Characteristics::ColdTime, Values::F64(0.9));
            m.add( "video_processing.0.0.1".to_string(), Characteristics::WarmTime, Values::F64(0.6));

            m.add( "video_processing.0.1.1".to_string(), Characteristics::ExecTime, Values::F64(0.4));
            m.add( "video_processing.0.1.1".to_string(), Characteristics::ColdTime, Values::F64(1.9));
            m.add( "video_processing.0.1.1".to_string(), Characteristics::WarmTime, Values::F64(1.6));

            m.add( "json_dump.0.1.1".to_string(), Characteristics::ExecTime, Values::F64(0.4));
            m.add( "json_dump.0.1.1".to_string(), Characteristics::ColdTime, Values::F64(1.9));
            m.add( "json_dump.0.1.1".to_string(), Characteristics::WarmTime, Values::F64(1.6));
        };
        
        // Test 1 single entries 
        push_video();
        println!("--------------------------------------------------------------------");
        println!("Test 1: Singular additions");
        println!("      : lookup ExecTime of json - {}", unwrap_val_f64(
                &m.lookup("json_dump.0.1.1".to_string(), Characteristics::ExecTime).unwrap() ) );
        println!("      : dumping whole map");
        m.dump();
        assert_eq!(unwrap_val_f64(
                     &m.lookup("json_dump.0.1.1".to_string(), Characteristics::ExecTime).unwrap() ),
                     0.4 );

        // Test 2 blind update to accumulate
        /*
        println!("--------------------------------------------------------------------");
        println!("Test 2: addition of ExecTime 0.5 to vp.0.1.1 - should be inplace update ");
        println!("      : dumping whole map");
        m.add( "video_processing.0.1.1".to_string(), Characteristics::ExecTime, Values::F64(0.5));
        m.dump();
        assert_eq!(unwrap_val_f64(
                     &m.lookup("video_processing.0.1.1".to_string(), Characteristics::ExecTime).unwrap() ),
                     0.5 );
                     */

        // Test 3 exponential average to accumulate
        m.add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::F64(0.5)) 
         .add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::F64(0.5))
         .add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::F64(0.5));
        println!("--------------------------------------------------------------------");
        println!("Test 3: three additions of ExecTime 0.5 to vp.0.0.1 - should be exponential average");
        println!("      : dumping whole map");
        m.dump();
        assert_eq!(unwrap_val_f64(
                     &m.lookup("video_processing.0.0.1".to_string(), Characteristics::ExecTime).unwrap() ),
                     0.48719999999999997 );

        // Test 4 using Duration datatype for ExecTime 
        let m = CharacteristicsMap::new( AgExponential::new( 0.6 ) );
        println!("--------------------------------------------------------------------");
        println!("Test 4: Using Duration Datatype for ExecTime");
        
        println!("      : Adding one element");
        m.add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::Duration(Duration::new(2,30)));
        println!("      : looking up the new element");
        println!("      :   {:?}", unwrap_val_dur(
                &m.lookup("video_processing.0.0.1".to_string(), Characteristics::ExecTime).unwrap() ) );
        println!("      : Adding three more");
        m.add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::Duration(Duration::new(5,50)));
        m.add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::Duration(Duration::new(5,50)));
        m.add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::Duration(Duration::new(5,50)));
        println!("      : dumping whole map");
        m.dump();
        assert_eq!(unwrap_val_dur(
                     &m.lookup("video_processing.0.0.1".to_string(), Characteristics::ExecTime).unwrap() ),
                     Duration::from_secs_f64(4.808000049) );

        return Ok(());
        
        /*
        // average of last four values
        let mut m = CharacteristicsMap::new( AgAverage::new(4) );

        // Test 4 simple average to accumulate
        m.add( "json_dump.0.1.1".to_string(), Characteristics::ExecTime, Values::F64(0.5));
        m.add( "json_dump.0.1.1".to_string(), Characteristics::ExecTime, Values::F64(0.5));
        m.add( "json_dump.0.1.1".to_string(), Characteristics::ExecTime, Values::F64(0.5));
        println!("Test 4: three additions of ExecTime 0.5 to j.0.1.1 - should be simple average");
        println!("      : dumping whole map");
        m.dump();

        // Test 5 adding different types for different characteristics 
        */
    }
}
