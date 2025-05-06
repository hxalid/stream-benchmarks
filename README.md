# NATS Jetstream Performance Benchmark

This repository contains benchmarking tools for evaluating NATS Jetstream performance across distributed environments. The benchmark tests message throughput and latency with the following configuration:

- Architecture: Multiple subjects distributed across streams, with streams distributed across NATS domains
- Producers: M concurrent producers (where M < N)
- Consumers: N concurrent ephemeral consumers
- Subject Distribution: Subjects are equally distributed among producers and consumers

This configuration allows for comprehensive performance testing of fan-out messaging patterns in distributed NATS Jetstream deployments. The use of ephemeral consumers ensures that subscription state is not persisted, simulating scenarios where consumers may connect and disconnect frequently.

Additionally, an alternative implementation using NATS Jetstream republish functionality is available in the repository at https://github.com/hxalid/stream-benchmarks/blob/main/standalone-tests/republish/main.go. This implementation demonstrates how the republish feature can be leveraged for message routing and transformation.

It can be executed as 

```
 go run standalone-tests/per-subject-consumers/main.go  -concurrency 30 -duration 1m -producers 10 -consumers 50 -batch 500 -consumeBatch 1000  -nats "nats://localhost:4222" -replicas 1  -shards 3 -size 2048  -natsPassword acc -natsUser acc -domains 1
```
