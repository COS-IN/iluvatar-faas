# Energy Monitoring


## Power cap

A power-capping feature has been implemented where the worker monitors system-level usage and can force queue invocations if power usage exceeds a certain limit.
The code can be found [here](../iluvatar_worker_library/src/services/invocation/energy_limiter.rs).
Two policies exist for it that are described in the `PowerCapVersion` enum.
An energy measuring source, either IPMI or RAPL must also be configured to use this feature at runtime.

This is a compile-time feature, and can be enabled with an argument passed to `make`.

```bash
make cargo_args='--features "power_cap"'
```