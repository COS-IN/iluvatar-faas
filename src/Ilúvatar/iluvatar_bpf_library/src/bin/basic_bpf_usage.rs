use iluvatar_bpf_library::bpf::func_characs::*;
use std::mem::MaybeUninit;

pub fn main() {
    let mut open_object = MaybeUninit::uninit();
    let skel = build_and_load(&mut open_object).unwrap();
    let fcmap = skel.maps.func_metadata;

    let key: BPF_FMAP_KEY = build_bpf_key("first_key");
    let key2: BPF_FMAP_KEY = build_bpf_key("second_key");

    let val = CharVal {
        prio: 1,
        e2e: 2,
        loc: 3,
    };
    update_map(&fcmap, &key, &val);
    update_map(&fcmap, &key2, &val);

    use std::{thread, time};
    let one_sec = time::Duration::from_millis(1000);
    loop {
        thread::sleep(one_sec);
    }
}
