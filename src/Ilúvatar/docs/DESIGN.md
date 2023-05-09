# Design

Ilúvatar is built in three primary parts, a controller, worker, and load generator.
It also includes two secondary pieces, a time series database and a standalone energy monitor.

SYSTEM DIAGRAM HERE

## Worker

The focus of the Ilúvatar system.
Information on running the Ilúvatar worker can be found [here](docs/WORKER.md).

## Controller

[Controller documentation](docs/CONTROLLER.md)

## Load Generator

Advanced load generation to both controller and workers can be found [here](docs/LOAD.md).

## Time series Database

We make use of a third-party time series database, `Influx`, to pool metrics reported by the workers and controller.
This enables online policymaking using information from workers, without the excessive overhead of communication between all nodes.

## Energy Monitor

The worker has built-in capability to monitor the system energy usage from various sources, RAPL, IPMI, etc.
However, there are cases where one wants to capture that information in the same format without running the worker.
The [energy monitor executable](./ENERGY.md) provides that functionality.
