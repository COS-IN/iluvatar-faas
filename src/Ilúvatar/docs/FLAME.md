# Flame Graphs

Understanding where time is spent inside an application can be challenging.
Several solutions for this exist in Rust.
As we use [tracing](https://docs.rs/tracing/latest/tracing/) to handle logging, we use an extension of it for low-effort flame graph creation.
The crate used to capture and generate the data is [here](https://docs.rs/tracing-flame/latest/tracing_flame/index.html).

Install the crate:

```bash
cargo install inferno
```

Set the `flame` config option in the logging section to something like `worker.flame`.
Build the worker with full spanning enabled to capture more span events, either `make spansd` (debug) or `make spans` (release).
After running the worker as long as you want, take the output data and send it to the crate's executable to convert to a flame graph like this.

```bash
cat /tmp/iluvatar/logs/worker.flame | inferno-flamegraph > tracing-flamegraph.svg
```
