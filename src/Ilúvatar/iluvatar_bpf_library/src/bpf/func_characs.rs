use anyhow::Result;

use std::fs;
use std::mem::MaybeUninit;
use std::path::Path;

use libbpf_rs::skel::OpenSkel;
use libbpf_rs::skel::SkelBuilder;
//use libbpf_rs::ErrorExt;
use libbpf_rs::MapCore;
use libbpf_rs::MapFlags;

#[allow(clippy::never_loop)]
#[allow(clippy::match_single_binding)]
mod charmap {
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/bpf/charmap.skel.rs"));
}
pub use charmap::CharmapSkel;
use charmap::*;
use std::fmt::{Debug, Error, Formatter};

impl Debug for CharmapSkel<'_> {
    // Required method
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.debug_struct("BPF Program").finish()
    }
}

#[allow(non_camel_case_types)]
pub type BPF_FMAP_KEY = [u8; 15];

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct CharVal {
    pub prio: u32,
    pub e2e: u32,
    pub loc: u32,
}

/// # Safety
/// This function takes the reference and generates a raw pointer to the data.
/// It should be used with caution as it can lead to undefined behavior if the data is not valid.
pub unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    ::core::slice::from_raw_parts((p as *const T) as *const u8, ::core::mem::size_of::<T>())
}

pub fn build_and_load(open_object: &mut MaybeUninit<libbpf_rs::OpenObject>) -> Result<CharmapSkel<'_>> {
    let mut skel_builder = CharmapSkelBuilder::default();
    skel_builder.obj_builder.debug(true);
    let open_skel = skel_builder.open(open_object)?;
    let mut skel = open_skel.load()?;

    let path = "/sys/fs/bpf/func_metadata";
    if Path::new(path).exists() {
        let _ = fs::remove_file(path);
    }

    let fcmap = &mut skel.maps.func_metadata;

    fcmap.pin(path).expect("failed to pin map");
    assert!(Path::new(path).exists());

    Ok(skel)
}

pub fn build_bpf_key(key: &str) -> [u8; 15] {
    let mut keyb: BPF_FMAP_KEY = [0; 15];

    for (i, k) in key.bytes().enumerate() {
        if i < keyb.len() {
            keyb[i] = k;
        }
    }
    keyb
}

pub fn update_map<'obj>(map: &'obj libbpf_rs::MapMut<'obj>, key: &BPF_FMAP_KEY, val: &CharVal) {
    let val: &[u8] = unsafe { any_as_u8_slice(val) };
    match map.update(key, val, MapFlags::ANY) {
        Ok(_) => (),
        Err(e) => {
            println!("error: unable to update the map {:?}", e);
        }
    }
}
