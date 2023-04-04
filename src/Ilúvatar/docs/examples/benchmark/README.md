# Benchmark

This example is a benchmarking run of the functions that come packaged with the Ilúvatar platform against the worker.
Each is run a number of times to get a representation of their cold and warm execution performance.

It also creates a `worker_function_benchmarks.json` with a subset of the result information that can be used in more advanced load generation scenarios.
It contains the warm and cold start end-to-end and execution times for every function that was benchmarked.
Examples of using this file are found in [cluster-trace](../cluster-trace/), [azure-trace](../azure-trace/), and [simulation](../simulation/).

Make sure you've updated the [host_addresses file](../../../ansible/group_vars/host_addresses.yml) with your machine's networking interface.
Simply execute `./run.sh` to run the example.
