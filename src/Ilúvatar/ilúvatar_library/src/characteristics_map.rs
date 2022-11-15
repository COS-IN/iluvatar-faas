use std::dashmap::DashMap;


////////////////////////////////////////////////////////////////
//! Aggregators for CharacteristicsMap 
trait Aggregator {
    fn accumulate<T> ( &self, oldvale: T, newvalue: T ) -> Self;
}

pub struct AgExponential {
    alpha: f64
}

impl Aggregator for AgExponential {

    fn new( alpha: f64 ) -> Self {
        AgExponential {
            alpha
        }
    }

    fn accumulate<T> ( &self, oldvalue: T, newvalue: T ) -> T {
        ( newvalue * alpha ) + ( oldvalue * (1-alpha) ) 
    }
}

////////////////////////////////////////////////////////////////
//! CharacteristicsMap Implementation  

enum Values {
    f64,
    u64,
    String
}

pub enum Characteristics {
    exec_time,
    warm_time,
    cold_time,
    memory_usage
}

pub struct CharacteristicsMap {
    map: DashMap<String,DashMap>,
    ag: Aggregator
}

// methods for the characteristics_map
//  new
//  add
//  lookup

impl CharacteristicsMap {
    pub fn new( ag: Aggregator ) -> Self {
        let mut map = CharacteristicsMap {
            map: DashMap<String,DashMap>::new(),
            ag
        };
        // TODO: Implement file restore functionality here 
        
        map
    }

    pub fn add<T> ( &self, fname: String, chr: Characteristics, value: T, accumulate: Option<bool> ) -> Self {
        let mut g0 = map.get( fname );
        let accumulate = accumulate.unwrap_or( true );

        match g0 {
            // dashmap of given fname
            Some(v0) => {
               let mut g1 = v0.get( chr );
                // entry against given characteristic
               match g1 {
                   Some(v1) => {
                       if accumulate {
                           *v1 = self.ag.accumulate( *v1, value );
                       } else {
                           *v1 = value;
                       }
                   },
                   None => {
                       v0.insert( chr, value );
                   }
               }
            },
            None => {
                // dashmap for given fname does not exist create and populate
                map.insert( fname, DashMap< Characteristics, Values >::new() )
            }
        }

        self
    }
    
    pub fn lookup<T> ( fname: String, chr: Characteristics ) -> Option<T> {
       let g0 = map.get( fname )?;
       g0.get( chr )
    }
}

#[cfg(test)]
mod charmap {
    use super::*;
    
    #[test]
    fn everything() -> Result<(), String> {
        let mut m = CharacteristicsMap::new( AgExponential::new( 0.6 ) );
        
        fn push_video() {
            m.add( "video_processing.0.0.1", exec_time, 0.3 );
            m.add( "video_processing.0.0.1", cold_time, 0.9 );
            m.add( "video_processing.0.0.1", warm_time, 0.6 );

            m.add( "video_processing.0.1.1", exec_time, 0.4 );
            m.add( "video_processing.0.1.1", cold_time, 1.9 );
            m.add( "video_processing.0.1.1", warm_time, 1.6 );

            m.add( "json_dump.0.1.1", exec_time, 0.4 );
            m.add( "json_dump.0.1.1", cold_time, 1.9 );
            m.add( "json_dump.0.1.1", warm_time, 1.6 );
        }
        
        // Test 1 single entries 
        push_video();
        println!("Test 1: Singular additions");
        println!("      : lookup exec_time of json - {}", m.lookup("json_dump.0.1.1", exec_time) );
        println!("      : dumping whole map");
        m.dump();

        // Test 2 blind update to accumulate
        m.add( "video_processing.0.1.1", exec_time, 0.5, Some(false) );
        println!("Test 2: addition of exec_time 0.5 to vp.0.1.1 - should be inplace update ");
        println!("      : dumping whole map");
        m.dump();

        // Test 3 exponential average to accumulate
        m.add( "video_processing.0.0.1", exec_time, 0.5 );
        m.add( "video_processing.0.0.1", exec_time, 0.5 );
        m.add( "video_processing.0.0.1", exec_time, 0.5 );
        println!("Test 3: three additions of exec_time 0.5 to vp.0.0.1 - should be exponential average");
        println!("      : dumping whole map");
        m.dump();
        
        return Ok(());
        // average of last four values
        let mut m = CharacteristicsMap::new( AgAverage::new(4) );

        // Test 4 simple average to accumulate
        m.add( "json_dump.0.1.1", exec_time, 0.5 );
        m.add( "json_dump.0.1.1", exec_time, 0.5 );
        m.add( "json_dump.0.1.1", exec_time, 0.5 );
        println!("Test 4: three additions of exec_time 0.5 to j.0.1.1 - should be simple average");
        println!("      : dumping whole map");
        m.dump();

        // Test 5 adding different types for different characteristics 
    }
}
