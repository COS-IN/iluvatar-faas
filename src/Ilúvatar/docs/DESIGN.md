# Design

Three custom-implemented parts, plus one external piece, make up the design of 

SYS DIAG HERE

## Worker

The focus of the Ilúvatar system.
Information on running the Ilúvatar worker can be found [here](docs/WORKER.md).

## Controller

[controller documentation](docs/CONTROLLER.md)

## Load Generator

Advanced load generation to both controller and workers can be can be found [here](docs/LOAD.md).

## Timeseries Database

We make use of a third-party timeseries, `Graphite`, to pool metrics reported by the workers and controller.
This enables online policy making using information from workers, without the excessive overhead of communication between all nodes.
