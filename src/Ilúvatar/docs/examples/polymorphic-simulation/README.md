# Simulation

Having a large cluster of machines, or running a plethora of configurations on physical machines is time-consuming as experiments require exclusive access to those resources.
This example runs the exact same trace with matching worker and controller configuration as in the [cluster trace example](../cluster-trace/).

The setup isn't exactly identical, as we run with three simulated workers instead of one live one.
Invocations are still sent via the controller and load-balanced to our workers, but no real system resources are made and functions "run" as sleep statements.

Simply execute `./run.sh` to run the example.
