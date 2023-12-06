# Research Possibilities

## Advanced Options

All configuration for the worker and controller are located in two central structs, one for each piece.
The [controller is much simpler](../iluvatar_controller_library/src/controller/controller_config.rs) and allows for changing the load balancing algorithm and what load metric might be used to balance invocations.

The [worker configuration](iluvatar_worker_library/src/worker_api/worker_config.rs) has many more options as it can micromanage invocations in many ways.
The primary struct, `Configuration`, contains many sub-structures.
`ContainerResourceConfig` has generic resources the container pool is allowed to use to store running and warm containers.
`ComputeResourceConfig` allows overloading CPU resources, and enabling GPU resources if the hardware is available.
Adjusting the queing policy can be done with `InvocationConfig`.
The network cache pool can be changed with the `NetworkingConfig` struct.

Details are not described here to avoid repeating information that is detailed in the code comments, and preventing inaccurate and out-of-date details.
These config options allow changing how the internals of the worker operate, and can be easily added to and modified.

## Configuration Exploration Experiments

The [paper-repro example](./examples/paper-repro/) runs several simulation experiments and plots the data in the same way Figure 10 of our Ilúvatar paper.
It runs different queue and resource configurations, easily changable, to explore the state space to test out different settings without large compute usage.
