# Server Scaling Compare

This example runs a single specified function against the worker from an increasing number of clients.
It does this by running more threads that connect and issue invocations in a closed-loop manner.

This extends the `scaling` example by comparing two different servers that can run inside each container, which are how the worker connects to them to run invocations.
One is an HTTP server, and the other listens on Unix sockets.

Make sure you've updated the [host_addresses file](../../../ansible/group_vars/host_addresses.yml) with your machine's networking interface.
Simply execute `./run.sh` to run the example.
