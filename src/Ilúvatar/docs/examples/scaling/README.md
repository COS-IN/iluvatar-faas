# Scaling

This example runs a single specified function against the worker with increasing throughput.
It does this by running more client threads that connect and issue invocations in a closed-loop manner.

The outputs are a series of files that denote how many concurrent clients there were for the experiment session.
Inside is the per-client (thread) set of invocations it was able to complete.

Make sure you've updated the [host_addresses file](../../../ansible/group_vars/host_addresses.yml) with your machine's networking interface.
Simply execute `./run.sh` to run the example.
