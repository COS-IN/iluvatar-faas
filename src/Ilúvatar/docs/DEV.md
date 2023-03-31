# Ilúvatar Developer Guidelines

## Setup

Following the commands in `setup.sh` to prepare the required dependencies.

## Code Standards

Some standards to follow to keep the codebase consistent.

### Documentation



### Warnings

Code **must** compile without warnings

### Error Handling

`Result` objects from _external_ libraries or RPC/HTTP **must** be extracted and converted to the success object or an Ilúvatar specific error or message.
Any error `Result` objects from _internal_ function calls can be propogated up without handling via `?`.

Combining an error log while returning an `Err` can be done with the custom macro `bail_error!`.

`panic!` must _only_ be used in startup code.
Any error encountered during startup should cause an exit.

### Logging

ALL functions that log something **must** take a `&iluvatar_library::transaction::TransactionId` paramater, typically named `tid`.
Maintaining this `TransactionId` across the execution path of a request will enable efficient correlation of events in the system.
This `TransactionId` **must** be placed in log messages.
The `tracing` crate enables json and structured logging, future log messages should pass the `TransactionId` via a `tid` argument.
This looks like:
```rust
debug!(tid=%tid, query=%url, "querying graphite render");
```

## Automated Testing

New code and features should add tests to ensure correctness and identify future errors.
