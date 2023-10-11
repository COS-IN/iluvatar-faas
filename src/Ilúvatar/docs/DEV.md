# Ilúvatar Developer Guidelines

## Setup

Following the commands in [setup](./SETUP.md#build-setup) to prepare the required build dependencies.

## Code Overview

### Rust Crate Breakdown

The source code is split into a variety of Rust crates, which all start with `ilúvatar_`.

* `ilúvatar_library` - Code that is not specific to any executable, or to be shared by all executables, To be generically usable everywhere.
* `ilúvatar_controller` - The binary crate for the controller. Only startup code should be here.
* `ilúvatar_controller_library` - All domain and logic code for the controller goes in here, to be reusable by both the `ilúvatar_controller` and `ilúvatar_load_gen`.
* `ilúvatar_worker` - The binary crate for the worker. Only startup code should be here.
* `ilúvatar_worker_library` - All domain and logic code for the worker goes in here, to be reusable by the `ilúvatar_controller_library`, `ilúvatar_worker` and `ilúvatar_load_gen`.
* `ilúvatar_load_gen` - A load generator that is capable of targeting either a worker or controller in a live or simulated manner.
* `ilúvatar_worker_cli` - A simple CLI to enable testing of the worker.
* `ilúvatar_energy_mon` - A standalone energy monitor that duplicates the energy tracking abilities of the worker.

The worker library is imported by the controller one to allow for sharing types and avoiding coding obscure constants and assumptions throughout the codebase

## Where to add code

### Controller

Generic controller code goes in the [primary folder](../ilúvatar_controller_library/src/controller/).
Adding new endpoints to the controller belong on the [web API](../ilúvatar_controller_library/src/controller/web_server.rs).
Load balancing polices need to implement the trait `LoadBalancerTrait` and are all located [here](../ilúvatar_controller_library/src/services/load_balance/).

### Worker

Changes to the worker's RPC API must edit the [proto file](../ilúvatar_worker_library/src/rpc/iluvatar_worker.proto).
Then the [API wrapper trait](../ilúvatar_worker_library/src/rpc/iluvatar_worker.proto) must be updated to support the new changes, plus the various implementers of the API.
This trait API allows easing code interaction between live and simulation versions of Ilúvatar.

Regularly logging metrics from inside should be added to the [global status service](../ilúvatar_worker_library/src/services/status/status_service.rs) that is logs them all periodically.
Modifications to the invocation pipeline and deciding how/where/when to run an invocation belong in [invocation](../ilúvatar_worker_library/src/services/invocation/).

Adding a new isolation or compute mechanism requires adding to the [container](../ilúvatar_worker_library/src/services/containers/) abstraction.
It must implement the isolation trait [ContainerIsolationService](../ilúvatar_worker_library/src/services/containers/mod.rs).
This abstracts the creation, deletion, and interaction with the specific isolation/compute mechanisms the container will provide.
It will then have to implement [ContainerT](../ilúvatar_worker_library/src/services/containers/structs.rs) on a struct to handle the details of running an invocation.
You may have to edit the [container manager](../ilúvatar_worker_library/src/services/containers/containermanager.rs) to handle the new isolation or compute.
Compute exclusivity is currently being manager for CPU and GPU using custom structs found [here](../ilúvatar_worker_library/src/services/resources/mod.rs).

## Code Standards

Some standards to follow to keep the codebase fairly consistent.
Coding style should follow Rust standards.
Things like improper casing will result in warnings from the compiler should be fixed unless there is a reason to override the warning.

New configuration that is expected to be frequently changed by users should be added to the [relevant Ansible file](../ansible/).
More details on how ansible works can be found in its [specific documentation](./ANSIBLE.md).

### Documentation

New code should be documented via [Rust documentation](https://doc.rust-lang.org/rust-by-example/meta/doc.html).

### Warnings

Code **must** compile without warnings.

### Error Handling

`Result` objects from _external_ libraries or RPC/HTTP **must** be extracted and converted to the success object or an Ilúvatar specific error or message.
Any error `Result` objects from _internal_ function calls can be propagated up without handling via `?`.

Combining an error log while returning `Err` can be done with the custom macro `bail_error!`, [found in here](../ilúvatar_library/src/macros.rs).

`panic!` must be avoided at all costs.
Errors at startup should be logged and propagated up, this will cause the app to exit.
Errors at runtime should be logged and return the error to the caller.
Background tasks should log and give up on what they were trying to do, but not exit.

### Logging

ALL functions that log something **must** take a `&iluvatar_library::transaction::TransactionId` parameter, typically named `tid`.
Maintaining this `TransactionId` across the execution path of a request will enable efficient correlation of events in the system.
This `TransactionId` **must** be placed in log messages.
The `tracing` crate enables JSON and structured logging, all log messages **must** pass the `TransactionId` via a `tid` argument.
This looks like:

```rust
debug!(tid=%tid, query=%url, "querying influx");
```

### Automated Testing

New code and features should add tests to ensure correctness and identify future errors.
Most tests are in [this folder](../ilúvatar_worker_library/tests/).

If you wish to run the automated tests, you need to make a custom `worker.dev.json` [here](../ilúvatar_worker_library/tests/resources/)
This file must be copied from the [master here](../ilúvatar_worker_library/tests/resources/worker.json) and only adjust the settings for the networking setup that enables it to connect with the host bridge.
Tests rely on the configuration values in the main file.
Once that is created, all tests can be run with `make test` in the `src/Ilúvatar` directory.

Tests that want different configuration from this baseline should run as a _simulation_.
For example the `sim_invoker_svc` function in the [testing utils file](../ilúvatar_worker_library/tests/utils.rs) creates the main worker services as a simulation, and can take arbitrary configuration overrides enabling this behavior.

Adding tests should be done in the closest part of the code possible.
Rust encourages putting unit tests [alongside source code](https://doc.rust-lang.org/book/ch11-01-writing-tests.html).
More complicated integration tests can go in a dedicated `tests` folder, which has been done for the [worker here](../ilúvatar_worker_library/tests/).
