
fn main() {
    scx_cargo::BpfBuilder::new()
    .unwrap()
    .enable_intf("src/bpf/intf.h", "bpf_intf.rs")
    .enable_skel("src/bpf/finesched.bpf.c", "bpf")
    .build()
    .unwrap();

    scx_cargo::BpfBuilder::new()
    .unwrap()
    .enable_skel("src/bpf/hashmaps.bpf.c", "bpf_hashmaps")
    .build()
    .unwrap();
}
