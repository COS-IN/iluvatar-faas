# Ilúvatar

Ilúvatar is an open Serverless platform built with the goal of jumpstarting and streamlining FaaS research.
It provides a system that is easy and consistent to use, highly modifiable, and directly reports experimental results.

If you use, extend, compare against, etc., Ilúvatar, please reference our HPDC 2023 paper in your work.

`Alexander Fuerst, Abdul Rehman, and Prateek Sharma. 2023. Ilúvatar: A Fast Control Plane for Serverless Computing.
In Proceedings of the 32nd International Symposium on High-Performance Parallel and Distributed Computing (HPDC ’23), June 16–23, 2023, Orlando, FL, USA.
ACM, New York, NY, USA, 14 pages. https://doi.org/10.1145/3588195.3592995`

## Design

The design is kept simple as to prevent unnecessary complexity when it comes to code management or deployment strategy.
More can be found [here](./docs/DESIGN.md).

## Setup

Information on preparing a node to run any of Ilúvatar's pieces can be found [here](docs/SETUP.md), along with how to set up a development machine.

## Examples

Quickly run a sample function using [these instructions](docs/FUNCTIONS.md).
Try out end-to-end examples [here](docs/examples/README.md).
Or run more advanced load scenarios using the [load generation documentation](docs/LOAD.md).

## Develop & Extend

Being a platform for research requires it to be easily extendible by future researchers.
Check out the [development documentation](./docs/DEV.md) for a breakdown of the codebase and how to implement features.

## Why 'Ilúvatar'?

The name 'Ilúvatar' comes from J.R.R. Tolkien's mythology, published in [*The Silmarillion*](https://tolkiengateway.net/wiki/The_Silmarillion), a history of the world prior to his *Lord of the Rings* books.
[Ilúvatar](https://tolkiengateway.net/wiki/Il%C3%BAvatar) is the creator of the world, and orchestrated its form and development.
We don't see our platform as being the ultimate, final, or conclusive FaaS platform.
But as a FaaS platform, it controls and directs where and how Serverless functions are created and executed, across a possibly great variety of heterogeneity and network distance.
Thus the inspiration for its name.

## More documentation

There are a number of minor features and options and details on them can be found [here](./docs/README.md).
