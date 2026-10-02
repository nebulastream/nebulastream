
# The Problem

NES already provides window aggregations, but synopsis-based analytics introduce requirements that go beyond ordinary scalar aggregation.

Condor is a framework for defining synopsis-based streaming jobs as stateful window aggregate functions. It classifies synopses by their algebraic properties and uses those properties to determine how to divide work, compute partial summaries, and merge them. A formal description of Condor and the problem it's solving was published at the [VLDB 2021](https://www.vldb.org/pvldb/vol14/p1818-poepsel-lemaitre.pdf.)
Its implementation is available at [TU-Berlin-DIMA/Condor](https://github.com/TU-Berlin-DIMA/Condor).

The Condor implementation targets Apache Flink. In that setting, workers belong to one cluster and can exchange partial summaries through the framework's distributed dataflow. NES instead targets a hierarchical sensor-edge-cloud topology. This creates new challanges to adapt all the proposed parallel execution strategies. Here is a list of the problems to solve:

**P1 - Synopses are not yet available as a common first-class library of NES operators.** Users who need approximate analytics may otherwise have to implement and maintain synopsis logic themselves. It's necessary to create interfaces that match the synopsis algebraic properties to allow for optimizations and allow for further extensions to user-defined synopses. 

**P2 - The Flink-based parallelization and merge strategy does not directly account for NES's hierarchical topology.** We must determine where partial synopses are computed and how and where they are merged across sensors, edge workers, and cloud workers.

**P3 - Include Condor's optimization strategies into NES optimizer.** Based on the classification of synopses and their algebraic properties, NES should leverage similar optimization strategies to improve the efficiency of synopsis-based streaming jobs.

**P4 - Overlapping windows can cause repeated processing.** Condor integrates general stream slicing through [Scotty](https://github.com/TU-Berlin-DIMA/scotty-window-processor) to improve performance for workloads with overlapping windows. NES should evaluate whether and how a comparable strategy fits its window execution model.

# Goals
**G1 — Provide a reusable synopsis abstraction and library.** Expose synopsis algorithms through a common interface suitable for NES operators, with the initial collection based on the 12 synopses provided by Condor. *(Addresses P1)*

**G2 — Make merging topology-aware.** Support merge plans that respect the sensor–edge–cloud hierarchy rather than assuming every worker is a peer in one flat cluster. *(Addresses P2.)*

**G3 — Preserve NES execution and optimization compatibility.** Integrate with existing query planning, operator placement, distributed planning, and execution mechanisms instead of introducing a separate runtime. *(Addresses P3.)*

**G4 — Integrate with NES window processing.** Define how synopsis state is associated with windows, when results are emitted, and how overlapping windows may share computation. *(Addresses P4.)*


# Alternatives
- **A1:** Let sensors update local synopses and periodically merge them at higher levels in the hierarchy. This approach may reduce the communication overhead and exploit the hierarchical topology, but requires compute in the lowest layer of the hierarchy, which may be resource-constrained.

- **A2:** Sensors only collect raw data and forward it to edge or cloud and then we exploit the computational resources at these levels to allow for parallized processing of synopses. This approach reduces the computational burden on sensors but may increase communication overhead and latency for synopsis updates.

- **A3:** Always compute synopses at the cloud level. This approach would look very similar to what Condor does. 

- **A4:** Implement all alternatives and let the optimizer dynamically choose the best approach based on the current system state and workload characteristics. This approach provides maximum flexibility and adaptability but may introduce additional complexity in the optimizer and runtime system.


# Our Proposed Solution
## Overview

Introduce a synopsis abstraction and a library of synopsis implementations, then represent synopsis computation as a windowed logical operation in NES. NES should plan partial synopsis computation and merge operations using the synopsis's declared capabilities and the worker topology.

The proposed design separates three concerns:

1. **Synopsis definition:** how a synopsis consumes tuples, maintains state, merges compatible states, and evaluates a summary.
2. **Window semantics:** which tuples belong to each window and when a synopsis result becomes eligible for emission.
3. **Execution strategy:** where partial state is maintained and how summaries are combined across the sensor–edge–cloud topology.

This separation allows NES to reuse synopsis algorithms without inheriting Flink's cluster assumptions.

## Synopsis interface

The library should define a small, stable contract. Exact C++ types and ownership semantics should follow NES conventions and be decided during implementation.

Follow the same class organization as in the [Condor implementation](https://github.com/TU-Berlin-DIMA/Condor/tree/master/core/src/main/java/de/tub/dima/condor/core/synopsis). 

## Windowed synopsis operator

Expose synopsis computation as a logical windowed operation. The operator should identify:

- The synopsis implementation and its configuration.
- The input attributes used by the synopsis.
- The window definition and time attribute, where applicable.
- The output/evaluation requested by the query.
- Whether distributed partial computation and merging are supported, including any required preconditions.

NES's existing window aggregation machinery is the natural integration point to investigate. The design should not assume that the current aggregation implementation already supports synopsis-specific state or merge constraints; this must be confirmed during implementation.

## Parallelization and topology-aware merging

The optimizer should use the network topology and the synopsis capabilities to construct a valid merge plan. Conceptually, a synopsis may be updated near the source, merged at an edge worker, and then merged again at a cloud worker. The actual plan must depend on the deployed topology and must not introduce a merge at a node that cannot communicate with the relevant partial states.

The planner/runtime design needs to define:

1. **State ownership:** which worker owns each partial synopsis for a given window and partition.
2. **Merge placement:** which worker performs each merge and how its placement is represented in the plan.
3. **Merge compatibility:** which partial states may be combined and what configuration, partition, or window metadata must match.
4. **Emission:** how the system determines that it has received the partial states required to evaluate a window.
5. **Failure and reconfiguration behavior:** how missing, duplicated, or late partial results are handled, and what happens when topology or placement changes.

These are open design/research areas. Condor's Flink strategy provides an algorithmic starting point, but NES must adapt it to its own hierarchical execution model.

# Proof of Concept
The PoC should be deliberately small and validate the most uncertain architectural aspects before porting the entire library.

1. Select at least one mergeable synopsis from Condor and implement its NES state/update behavior.
2. Execute it over a single worker and verify window assignment and result emission.
3. Execute it over multiple workers with partial synopsis computation and merging.
4. Construct a hierarchical sensor–edge–cloud topology and verify that merge operations are placed along valid communication paths.
5. Compare results against a trusted single-worker computation for the same input and window definitions.
6. Benchmark overlapping windows with and without shared slicing, if a slicing prototype is available.
7. Record throughput, latency, state size, network bytes, and merge overhead.

The PoC should demonstrate correctness and feasibility; it should not claim production readiness or universal performance benefits.

# Summary
The proposed solution addresses **P1** and **G1** by introducing a reusable synopsis abstraction and library, initially based on Condor's 12 synopsis implementations. It addresses **P2** and **G2** by making state ownership, partial computation, and merge placement topology-aware across the sensor–edge–cloud hierarchy. It addresses **P3** and **G3** by integrating synopsis computation with NES's existing planning, placement, optimization, and execution mechanisms. Finally, it addresses **P4** and **G4** by defining synopsis state and result emission within NES windows and evaluating shared slicing for overlapping windows.

The design does not yet prescribe a single universal merge strategy, failure model, or slicing implementation. These depend on the synopsis capabilities, deployed topology, and existing NES execution semantics, so they are intentionally validated through the PoC rather than fixed prematurely. This is sufficient for the design because it establishes the required interfaces and integration points while leaving implementation-specific choices to measurement and further research.

Among the alternatives, **A4** is the best fit: it preserves the flexibility to place computation at sensors, edges, or the cloud according to resource, communication, latency, and workload conditions. However, **A4** is also the most complex to implement and optimize, requiring further research and careful consideration of trade-offs. The proposed solution therefore supports the useful strategies represented by **A1–A3** while allowing NES's optimizer to choose among them, rather than committing to one topology or resource assumption.


# Sources and Further Reading
- [Condor's VLDB paper writen by Poepsel-Lemaitre et. al.](https://www.vldb.org/pvldb/vol14/p1818-poepsel-lemaitre.pdf)
- [Condor's GitHub repository](https://github.com/TU-Berlin-DIMA/Condor)
- Cormode, Graham, et al. "Synopses for massive data: Samples, histograms, wavelets, sketches." Foundations and Trends® in Databases 4.1–3 (2011): 1-294.


