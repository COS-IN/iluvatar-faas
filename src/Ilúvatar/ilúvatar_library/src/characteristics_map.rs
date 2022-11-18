use dashmap::DashMap;
use std::time::Duration;

#[derive(Debug)]
pub enum Values {
    Duration(Duration),
    F64(f64),
    U64(u64),
    Str(String)
}

pub fn unwrap_val_dur ( value: &Values ) -> Duration {
    let stop = || panic!("unwrap_val_dur not of type Duration");
    match value {
        Values::Duration(v) => v.clone(), 
        _  => stop() 
    }
}

pub fn unwrap_val_f64 ( value: &Values ) -> f64 {
    let stop = || panic!("unwrap_val_f64 not of type f64");
    match value {
        Values::F64(v) => v.clone(), 
        _  => stop() 
    }
}

pub fn unwrap_val_u64 ( value: &Values ) -> u64 {
    let stop = || panic!("unwrap_val_u64 not of type u64");
    match value {
        Values::U64(v) => v.clone(), 
        _  => stop() 
    }
}

pub fn unwrap_val_str ( value: &Values ) -> String {
    let stop = || panic!("unwrap_val_str not of type String");
    match value {
        Values::Str(v) => v.clone(), 
        _  => stop() 
    }
}

////////////////////////////////////////////////////////////////
/// Aggregators for CharacteristicsMap 
pub struct AgExponential {
    alpha: f64
}

impl AgExponential {
    pub fn new( alpha: f64 ) -> Self {
        AgExponential {
            alpha
        }
    }

    fn accumulate ( &self, oldvalue: &Values, newvalue: &Values ) -> Values {
        match oldvalue {
            Values::F64(_v)=> {
                let old = unwrap_val_f64( oldvalue );
                let new = unwrap_val_f64( newvalue );
                Values::F64( ( new * self.alpha ) + ( old * (1.0-self.alpha) ) )
            }
            Values::Duration(_v) => {
                let old = unwrap_val_dur( oldvalue );
                let new = unwrap_val_dur( newvalue );
                Values::Duration(Duration::from_secs_f64( ( new.as_secs_f64() * self.alpha   ) + ( old.as_secs_f64() * (1.0-self.alpha) ) ))
            }
            _ => {
                panic!("AgExponential::accumulate does not support this value type");
            }
        }
        
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

    pub fn add( &self, fname: String, chr: Characteristics, value: Values, accumulate: Option<bool> ) -> &Self {
        let e0 = self.map.get_mut( &fname );
        let accumulate = accumulate.unwrap_or( true );

        match e0 {
            // dashself.map of given fname
            Some(v0) => {
               let e1 = v0.get_mut( &chr );
               let v;
               println!("Adding to {}", v0.key() );
                // entry against given characteristic
               match e1 {
                   Some(ref v1) => {
                       println!("        {:?} - {:?}", v1.key(), value );
                       if accumulate {
                           v = self.ag.accumulate( v1.value(), &value );
                       } else {
                           v = value; 
                       }
                   },
                   None => {
                       println!("doesn't already exist adding");
                       v = value;
                   }
               }
               drop(e1);
               v0.insert( chr, v );
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
                
                println!("{} -- {:?},{:?}", fname, chr, value);
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
            m.add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::F64(0.3), Some(true) );
            m.add( "video_processing.0.0.1".to_string(), Characteristics::ColdTime, Values::F64(0.9), Some(true) );
            m.add( "video_processing.0.0.1".to_string(), Characteristics::WarmTime, Values::F64(0.6), Some(true) );

            m.add( "video_processing.0.1.1".to_string(), Characteristics::ExecTime, Values::F64(0.4), Some(true) );
            m.add( "video_processing.0.1.1".to_string(), Characteristics::ColdTime, Values::F64(1.9), Some(true) );
            m.add( "video_processing.0.1.1".to_string(), Characteristics::WarmTime, Values::F64(1.6), Some(true) );

            m.add( "json_dump.0.1.1".to_string(), Characteristics::ExecTime, Values::F64(0.4), Some(true) );
            m.add( "json_dump.0.1.1".to_string(), Characteristics::ColdTime, Values::F64(1.9), Some(true) );
            m.add( "json_dump.0.1.1".to_string(), Characteristics::WarmTime, Values::F64(1.6), Some(true) );
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
        println!("--------------------------------------------------------------------");
        println!("Test 2: addition of ExecTime 0.5 to vp.0.1.1 - should be inplace update ");
        println!("      : dumping whole map");
        m.add( "video_processing.0.1.1".to_string(), Characteristics::ExecTime, Values::F64(0.5), Some(false) );
        m.dump();
        assert_eq!(unwrap_val_f64(
                     &m.lookup("video_processing.0.1.1".to_string(), Characteristics::ExecTime).unwrap() ),
                     0.5 );

        // Test 3 exponential average to accumulate
        m.add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::F64(0.5), Some(true) );
        m.add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::F64(0.5), Some(true) );
        m.add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::F64(0.5), Some(true) );
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
        m.add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::Duration(Duration::new(2,30)), Some(true) );
        println!("      : looking up the new element");
        println!("      :   {:?}", unwrap_val_dur(
                &m.lookup("video_processing.0.0.1".to_string(), Characteristics::ExecTime).unwrap() ) );
        println!("      : Adding three more");
        m.add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::Duration(Duration::new(5,50)), Some(true) );
        m.add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::Duration(Duration::new(5,50)), Some(true) );
        m.add( "video_processing.0.0.1".to_string(), Characteristics::ExecTime, Values::Duration(Duration::new(5,50)), Some(true) );
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
        m.add( "json_dump.0.1.1".to_string(), Characteristics::ExecTime, Values::F64(0.5), Some(true) );
        m.add( "json_dump.0.1.1".to_string(), Characteristics::ExecTime, Values::F64(0.5), Some(true) );
        m.add( "json_dump.0.1.1".to_string(), Characteristics::ExecTime, Values::F64(0.5), Some(true) );
        println!("Test 4: three additions of ExecTime 0.5 to j.0.1.1 - should be simple average");
        println!("      : dumping whole map");
        m.dump();

        // Test 5 adding different types for different characteristics 
        */
    }
}
