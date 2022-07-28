# Flame Graphs

The crate used to capture and generate the data is (here)[https://docs.rs/tracing-flame/latest/tracing_flame/index.html]
It can turn the spans created by the (tracing crate)[https://docs.rs/tracing/latest/tracing/] into a low-effort flame graph.

Install the crate:
```bash
cargo install inferno
```

Take the output data nad send it to the crate's executable to convery to the graph.

```bash
cat /tmp/ilúvatar/logs/worker.flame | inferno-flamegraph > tracing-flamegraph.svg
```
