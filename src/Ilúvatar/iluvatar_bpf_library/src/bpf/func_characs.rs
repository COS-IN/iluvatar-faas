use anyhow::bail;
use anyhow::Result;
use anyhow::Context;

use std::mem::MaybeUninit;
use std::path::Path;
use std::fs;

use libbpf_rs::skel::OpenSkel;
use libbpf_rs::skel::Skel;
use libbpf_rs::skel::SkelBuilder;
//use libbpf_rs::ErrorExt;
use libbpf_rs::MapCore;
use libbpf_rs::MapFlags;
use libbpf_rs::OpenObject;

mod charmap {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/bpf/charmap.skel.rs"
    ));
}
use charmap::*;
pub use charmap::CharmapSkel;
use std::fmt::{Debug, Formatter, Error};

impl Debug for CharmapSkel<'_> {
    // Required method
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error>{
        f.debug_struct("BPF Program")
            .finish()
    }
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct CharVal{
    pub prio: u32,
    pub e2e: u32,
    pub loc: u32,
}

pub unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    ::core::slice::from_raw_parts(
        (p as *const T) as *const u8,
        ::core::mem::size_of::<T>(),
    )
}

pub fn build_and_load<'obj>( open_object:  &'obj mut MaybeUninit::<libbpf_rs::OpenObject> ) -> Result<CharmapSkel<'obj>> {
    let mut skel_builder = CharmapSkelBuilder::default();
    skel_builder.obj_builder.debug(true);
    let open_skel = skel_builder.open(open_object)?;
    let mut skel = open_skel.load()?;

    let path = "/sys/fs/bpf/func_characs";
    if( Path::new(path).exists() ){
        let _ = fs::remove_file(path);
    }

    let fcmap = &mut skel
        .maps
        .func_characs;

    fcmap.pin(path).expect("failed to pin map");
    assert!(Path::new(path).exists());

    Ok(skel)
}

pub fn update_map<'obj>( map:  &'obj libbpf_rs::MapMut<'obj>, key: u64, val: &CharVal ) 
{
    let key = key.to_ne_bytes();
    let val: &[u8] = unsafe { any_as_u8_slice(val) };
    match map.update(&key, val, MapFlags::ANY) {
        Ok(_) => (),
        Err(e) => {
            println!("error: unable to update the map {:?}", e);
        }
    }
}






