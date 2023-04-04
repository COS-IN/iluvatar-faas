# Azure Trace

This complex example creates a realistic workload by pulling data from a [dataset of Azure Functions](https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsDataset2019.md).
It also uses the `worker_function_benchmarks.json` from the [benchmark](../benchmark/) to map the trace characteristics to actual code to run on the worker.
Once the dataset has been analyzed, a trace is generated using [function ECDFs](../../../../load/generation/azure/ecdf_trace.py) to give realistic invocation arrival times.

**NOTE:** this example can use significant system resources, especially when downloading and performing the dataset analysis.
It can use some 30 GB of disk space, and it is recommended to have at least 16 GB of memory before running.
Do not expect to be able to use your machine until it is complete, this only happens once after downloading as the results are cached.

Make sure you've updated the [host_addresses file](../../../ansible/group_vars/host_addresses.yml) with your machine's networking interface.
Simply execute `./run.sh` to run the example.
