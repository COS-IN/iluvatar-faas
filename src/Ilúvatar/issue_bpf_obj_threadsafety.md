# Thread Safety of bpf object 


  error log 
```
   368 | impl DeviceQueue for CpuQueueingInvoker {
    |                      ^^^^^^^^^^^^^^^^^^ `NonNull<libbpf_sys::bpf_object>` cannot be sent between threads safely
    |
    = help: within `libbpf_rs::object::OpenObject`, the trait `std::marker::Send` is not implemented for `NonNull<libbpf_sys::bpf_object>`
note: required because it appears within the type `libbpf_rs::object::OpenObject`
   --> /u/abrehman/.cargo/registry/src/index.crates.io-6f17d22bba15001f/libbpf-rs-0.24.4/src/object.rs:221:12
    |
221 | pub struct OpenObject {
    |            ^^^^^^^^^^
    = note: required for `std::sync::Mutex<libbpf_rs::object::OpenObject>` to implement `Sync`
note: required because it appears within the type `std::option::Option<std::sync::Mutex<libbpf_rs::object::OpenObject>>`
   --> /rustc/07dca489ac2d933c78d3c5158e3f43beefeb02ce/library/core/src/option.rs:570:10
note: required because it appears within the type `CharacteristicsMap`
   --> /data2/ar/iluvatar-faas/src/Ilúvatar/iluvatar_library/src/characteristics_map.rs:142:12

```
  
  * NonNull<libbpf_sys::bpf_object> - cannot be sent between threads safely because 
    * within `libbpf_rs::object::OpenObject`, the trait `std::marker::Send` is not implemented for `NonNull<libbpf_sys::bpf_object>`
      * it is required because 
        * for `std::sync::Mutex<libbpf_rs::object::OpenObject>` to implement `Sync` - it's needed 

  * there is no way to avoid this 
    * as to implement send for nonnull type in openobject would be modifying the libbpf_rs   
    * rather make it so that we have a single sink which doesn't require thread safety 
    * and multiple producers can push to it 
    * mpsc - higher order construct 



