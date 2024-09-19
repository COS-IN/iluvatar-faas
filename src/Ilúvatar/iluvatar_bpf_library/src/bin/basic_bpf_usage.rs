use iluvatar_bpf_library::bpf::func_characs::*;
use std::mem::MaybeUninit;

pub fn main() {

    let mut open_object = MaybeUninit::uninit();
    let mut skel = build_and_load( &mut open_object ).unwrap(); 
    let mut fcmap = skel
        .maps
        .func_metadata;

    // let key: u64 = 10;
    // let key2: u64 = 20;
    // let mut key: BPF_FMAP_KEY  = *b"first_key      ";
    // let mut key2: BPF_FMAP_KEY = *b"second_key     ";

    let mut key: BPF_FMAP_KEY  = build_bpf_key( &"first_key".to_string() );
    let mut key2: BPF_FMAP_KEY = build_bpf_key( &"second_key".to_string() );

    let val = CharVal{
        prio: 1,
        e2e: 2,
        loc:3 
    };
    update_map( &mut fcmap, &key, &val );
    update_map( &mut fcmap, &key2, &val );

    use std::{thread, time};
    let one_sec = time::Duration::from_millis(1000);
    loop {
        thread::sleep(one_sec); 
    }
}

