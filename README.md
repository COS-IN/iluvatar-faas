# Ilúvatar

Ilúvatar is an open Serverless platform built with the goal of jumpstarting and streamlining FaaS research.
It provides a system that is easy and consistent to use, highly modifiable, and directly reports experimental results.

<img src="./imgs/logo1.jpeg" alt="Ilúvatar orchestrating functions" width="300"/>

## Try it Out

```sh
cd src/Ilúvatar
./simple_setup.sh
sudo ./target/debug/ilúvatar_worker -c ilúvatar_worker/src/worker.dev.json &
worker_pid=$(echo $!)
```

Register a function with the worker.

```sh
./target/debug/ilúvatar_worker_cli --address "127.0.0.1" --port 8079 register --name "hello" --version 1 --image "docker.io/alfuerst/hello-iluvatar-action:latest" --memory 128 --cpu 1
```

Invoke the newly registered function, passing custom arguments.

```sh
./target/debug/ilúvatar_worker_cli --address "127.0.0.1" --port 8079 invoke --name "hello" --version 1 -a name=`whoami`
```

Kill the worker running in the background.

```sh
sudo kill --signal SIGINT $worker_pid
```

## Why use Ilúvatar?

There are several open-sourced FaaS platforms, but we believe none of them are ideal for research work.

1. Load generation built-in
2. Cluster support
3. Ease of enahancement
4. etc.

A popular open-source platform used in research is OpenWhisk, but we have found it has high overheads under notable load, caused by a variety of factors.
The scalability of OpenWhisk vs our Ilúvatar can be seen here.

![Ilúvatar orchestrating functions](./imgs/overhead-scaling.jpeg)

We can ensure a constant 1-3 ms overhead on invocations at significant load, whereas OpenWhisk sees high and variable overheads.
It also does not have the reasearch-first features implemented in Ilúvatar.

## Why 'Ilúvatar'?

The name 'Ilúvatar' comes from J.R.R. Tolkien's mythology, published in [*The Silmarillion*](https://tolkiengateway.net/wiki/The_Silmarillion), a history of the world prior to his *Lord of the Rings* books.
[Ilúvatar](https://tolkiengateway.net/wiki/Il%C3%BAvatar) is the creator of the world, and orchestrated its form and development.
We don't see our platform as being the ultimate, final, or conclusive FaaS platform.
But as a FaaS platform, it controls and directs where and how Serverless functions are created and executed, across a possibly great variety of heterogeneity and network distance.
Thus the inspiration for its name.

## More documentation

Detailed documentation can be found [here](./src/Ilúvatar/README.md).
Ilúvatar supports a large variety of customization in configuration and setup, and methods of load generation and experimentation.

## Citation

If you use, extend, compare against, etc., Ilúvatar, please reference our HPDC 2023 paper in your work.

`Alexander Fuerst, Abdul Rehman, and Prateek Sharma. 2023. Ilúvatar: A
Fast Control Plane for Serverless Computing. In Proceedings of the 32nd
International Symposium on High-Performance Parallel and Distributed Com-
puting (HPDC ’23), June 16–23, 2023, Orlando, FL, USA. ACM, New York, NY,
USA, 14 pages. https://doi.org/10.1145/3588195.3592995`
