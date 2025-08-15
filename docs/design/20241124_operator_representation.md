# The Problem
The query registration process we currently have consists of several intermediate representations, lowering, serialisation and optimization steps.
Because these steps have evolved slowly over time, they have never been streamlined into a single coherent process.
At the same time, we need to adapt our current operator representations to new design goals, namely extensibility and cost-based multiphase optimization.
In particular, these challenges affect different areas of our code base:

**P1:** The current physical operator representation.
Our system needs to be able to model a variety of properties that may affect the optimization and placement process:
Hardware properties (_AVX 512_, _GPU_), physical implementation used (_Hash_, _Sort_), data format such as compression (_SNAPPY_, _GZIP_).
In fact, with heterogeneity as a design goal, the list of important physical properties may grow in the future.
Moreover, these properties may overlap.
However, while representing them is crucial for effective query optimization, we are not able to express physical properties naturally.
In fact, our current implementation only performs simple 1-to-1 translations from logical to physical operator representations without any added value.

**P2:** The current lowering and serialization steps.
These steps are not currently supported by plugins.
Serialization is currently hardwired and only allows a fixed set of serializable operators.
The same is true for creating and lowering logical, physical, and nautilus operators.

**P3:** The current query optimizer.
When streamlining operator representations, we need to consider query optimization, as it is highly intertwined.
We need operator representations that can hold the current state of the query optimizer.
And these representations need to be exchanged between the coordinator (global) and worker (local) levels to allow distributed optimization between them.
Our current operator representations are not capable of representing the current progress of query optimization.
Sending the logical plan representation to worker nodes limits us to a small set of logical optimizations at the coordinator.
At the worker node, we do not perform any optimizations at all.

**P4:** The current query compiler.
It is a central class during query registration, including a fixed set of phases that transform the input query plan into an executable query plan.
Interestingly, the actual query compilation does not occur at this stage but later at query startup.
Additionally, the fixed set of phases does not include worker-local query optimization.

# Goals
The goal of this design document is to describe a query registration process that is streamlined and ready for extensibility and complex query optimization.

**G1:** Streamlined lowering and serialization.
The query registration has no redundancies or unintentional complexities.
While it is open for extension it is closed for modification.

**G2:** Plugins for operators and optimizations.
We allow plugin developers to introduce new operators and optimizations without modifying the core system.
Operators must have a standardized structure to ensure compatibility between internal and external operators.
The same should hold for custom optimization rules and physical properties.

**G3:** Allow modelling complex interleaved physical operator properties.
It should be flexible enough to allow also hierarchical operator structures, i.e., the support for sub-operators in query plans.

**G4:** Enabling a flexible cost-based multiphase optimizer.
We want our operators to be ready for cost-based multiphase optimization (e.g., Volcano/Cascade-like optimization [^2]).
Crucial is the ability to allow 1-to-N translations, i.e., translating a plan into multiple logically equivalent physical plans.
Based on that a query optimizer can select a cost-optimal plan.

**G5:** Operator representations that enable distributed, collaborative optimization.
Specifically, it should allow collaboration between global optimizations (focus on broader, cross-query, cross-device optimizations that improve system-wide performance) and
local optimizations (per-device optimizations with fresh, highly accurate statistics).
Since query optimization is very resource-intensive, local optimizers should be prevented from revisiting decisions already discarded by the global optimizer.
At the same time, the representation should be able to store the current state of the optimization.
It should allow the global optimization process to be paused and resumed on the local level.

# Non-Goals

**NG1:** Modelling the interaction/work distribution between the local and global optimization. This should be part of future research.

**NG2:** Modelling the details of the optimizer. This will be part of a separate design document.

# Solution Background
We provide background on our current operators implementation and multiphase cost-based optimizer.

## Current Implementation

1. The `AntlrQueryParser` creates a `QueryPlan` using `QueryPlanBuilder`.

   [https://github.com/nebulastream/nebulastream-public/blob/99be8c88de3e305a2ef25ff160d75ab0237ab362/nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp#L449-L502](https://github.com/nebulastream/nebulastream-public/blob/99be8c88de3e305a2ef25ff160d75ab0237ab362/nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp#L449-L502)

   The `QueryPlan` consists of implementations of `LogicalOperators` which themselves inherit from `Operator` and `Operator` inherits from `Node`.
   https://github.com/nebulastream/nebulastream-public/blob/cc251e482a848118bc2f06e86261ea83d84e6a74/nes-operators/include/Operators/LogicalOperators/LogicalOperator.hpp#L43-L123

2. Nebuli, our current coordinator equivalent, creates a `DecomposedQueryPlan.cpp`. During that process it applies a range of hard-wired optimization phases.
   https://github.com/nebulastream/nebulastream-public/blob/cc251e482a848118bc2f06e86261ea83d84e6a74/nes-nebuli/src/NebuLI.cpp#L180-L232

3. Nebuli serializes the DecomposedQueryPlan into a `SerializableQueryPlan.pb`.
   https://github.com/nebulastream/nebulastream-public/blob/cc251e482a848118bc2f06e86261ea83d84e6a74/nes-nebuli/src/NebuLIStarter.cpp#L239-L285

   The serialized operator representation is for each operator hard-coded in `SerializableOperator.proto`.
   https://github.com/nebulastream/nebulastream-public/blob/cc251e482a848118bc2f06e86261ea83d84e6a74/grpc/SerializableOperator.proto#L66-L107

4. The single node worker deserializes the query plan back into a `DecomposedQueryPlan`.
   https://github.com/nebulastream/nebulastream-public/blob/cc251e482a848118bc2f06e86261ea83d84e6a74/nes-single-node-worker/src/GrpcService.cpp#L39-L52

   The serialization and deserialization functions are for each operator hard-coded in the `OperatorSerializationUtil`.
   https://github.com/nebulastream/nebulastream-public/blob/cc251e482a848118bc2f06e86261ea83d84e6a74/nes-operators/include/Operators/Serialization/OperatorSerializationUtil.hpp#L53-L76

5. The `QueryCompiler` gets the `QueryCompilationRequest` containing the `DecomposedQueryPlan` and performs the various lowering stages,
   [https://github.com/nebulastream/nebulastream-public/blob/99be8c88de3e305a2ef25ff160d75ab0237ab362/nes-execution/src/QueryCompiler/QueryCompiler.cpp#L58-L84](https://github.com/nebulastream/nebulastream-public/blob/99be8c88de3e305a2ef25ff160d75ab0237ab362/nes-execution/src/QueryCompiler/QueryCompiler.cpp#L58-L84)

   1. `LowerLogicalToPhysicalOperatorsPhase` replaces operators with their physical operator equivalent.
      https://github.com/nebulastream/nebulastream-public/blob/cc251e482a848118bc2f06e86261ea83d84e6a74/nes-execution/include/QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp#L29-L40

   2. `PipeliningPhase` creates a `PipelineQueryPlan` including `OperatorPipeline`.
      https://github.com/nebulastream/nebulastream-public/blob/cc251e482a848118bc2f06e86261ea83d84e6a74/nes-execution/include/QueryCompiler/Operators/PipelineQueryPlan.hpp#L28-L49

   3. `AddScanAndEmitPhase` adds scan and emit operators to each pipeline.

   4.  `NautilusCompilationPhase` replaces the `PipelineOperators` with `ExecutableOperator`s with `NautilusPipelineOperator`s.

   5. `LowerToExecutableQueryPlanPhase` creates an `ExecutableQueryPlan` containing `SuccessorExecutablePipeline`s.

6. The query is now registered. Currently, the compilation does not happen at the registration in the `QueryCompiler` but during query startup in the `CompiledExecutablePipelineStage::setup()` method.
   https://github.com/nebulastream/nebulastream-public/blob/cc251e482a848118bc2f06e86261ea83d84e6a74/nes-execution/src/Execution/Pipelines/CompiledExecutablePipelineStage.cpp#L79-L87


## Volcano Optimizer
Goetz Graefe`s work on the Volcano, Cascades and Exodus optimizer is considered as
> "one of the two reference architectures for query optimization from the early days of database research that cover most of the serious optimizer implementations today" [^4] (besides System R).

A state-of-the-art implementation of Volcano's design principles is Calcite.

**Operator Traits**

Volcano's first design goal is extensibility, and there is overlap with that of NebulaStream, as we also strive for extensibility.
To achieve that, among Volcano's main concepts are operator properties, called operator traits in Calcite:
> "Calcite does not use different entities to represent logical and physical operators.
Instead, it describes the physical properties associated with an operator using traits." [^1]

In both systems, during optimization each operator has a set of traits.
Traits allow the optimizer to reason about the physical execution environment (e.g., parallelism, partitioning, ordering) without changing logical semantics.

Traits solely change physical properties:
> "Changing a trait value does not change the logical expression being evaluated, i.e., the rows produced by the given operator will still be the same." [^1]

During optimization an operator may require its input to satisfy a specific physical condition (e.g., ordered by column X).
To check whether the condition is satisfied, operators expose physical traits.
Based on that the optimizer might propagate, enforce, compare and validate operator traits.

**Actions with Traits**

If an operator’s input does not naturally provide the required trait (e.g., ordering, partitioning), the optimizer can insert an enforcer operator to produce the required trait.
For instance, adding a sort operator if the upstream operator does not provide sorted output.
In other words:
> "There are some operators in the physical algebra that do not correspond to any operator in the logical algebra, for
example sorting and decompression. The purpose of these operators is not to perform any logical data manipulation
but to enforce physical properties in their outputs that are required for subsequent query processing algorithms. We
call these operators enforcers; they are comparable to the "glue" operators in Starburst." [^3]

The optimizer can determine which traits are supported by operators in the plan and pass these requirements downward or upward through the plan.
For example, an operator might require its input to be sorted (ordering trait), and this requirement can be propagated to its child operators to ensure they produce sorted output.
Based on these propagations new enforcements might be needed.

The optimizer compares the desired traits of an operator's inputs with the traits the inputs currently provide.
If there is a mismatch, it triggers either the insertion of enforcers or the invocation of different rules to align the traits.

Rules are also used to lower logical plans to physical implementations.
Rules have an `onMatch()` callback function.
A rule includes an operator tree that specifies when a rule should 'fire'.
Collection of rules organised into different planning phases.

**Volcano Optimizer Phases**

Typically, query optimization consist of multiple phases in which transformation rules are triggered.
Here, a short outline of a Volcano-based optimizer to understand how traits and rules are used:

1. **Exploration Phase**
   Transformation rules that the planner uses to generate equivalent logical plans.
   We form equivalence groups between operator trees.
   In the end, we have a set of logically equivalent plans organized into equivalence groups.
   Search strategy to systematically explore the space of plans.
   It often uses a top-down or bottom-up search (or both phases sequentially), driven by rules that trigger transformations.
   Volcano optimizers often have a queue of pending rule applications, along with heuristics for scheduling these rules to ensure termination and avoid endless transformations.

   A memo data structure stores all equivalence groups to represent logically equivalent plans.
   By using the memo, the optimizer efficiently avoids redundant explorations.

   Here, the optimizer saturates the search space with logical transformations and then later introduces physical variants with specific traits.
   Once the search space of logical alternatives is fully explored, the planner switches to considering physical operators and enforcing the traits needed for efficient execution.

2. **Implementation Phase**
   Once transformation rules are exhausted, Calcite begins exploring physical implementations for each logical operator.
   We have to do trait propagation.
   The planner verifies that the final selected plan satisfies all required traits before execution.
   During the Implementation Phase, cost estimation and cost-based pruning are performed to discard non-promising plans.

3. **Final Plan Selection**
   The optimizer selects the plan with the lowest cost.

The interested reader is referred to the original Volcano [^3] and Calcite [^1] papers.

# Our Proposed Solution

We propose the following solutions:
- S1: Operator traits
- S2: Operator representations
- S3: Query compiler
- S4: Query registration process

## S1: Operator Traits
We move away from modeling physical operators explicitly via their own classes but rather introduce our own operator trait system.
Our logical operator get more and more physical properties through the lowering and optimization process, gradually becoming more physical.
This allows very flexible query optimization approaches as there is no concrete mapping of types to components leading to a case in which novel optimizer strategies are difficult to implement.
The differentiation rather happens by the registered rules, i.e., the local optimizer includes rules that can consider the local statistics.
This allows us to 'stop' the query optimization query plan at all stages of the optimization.
At the global optimization traits the optimizer can send at plan to the local optimizations that be used to enforce certain optimizations.

This is done by applying rules that transform the plan based on the operator class and the associated (physical) traits.
Traits can model any physical properties of relational operators, except properties that can change during runtime, i.e., runtime metrics/statistics.

**Convention Traits:**

There is one property that is mandatory for each operator: the convention property.
It describes the main property of how the operator is implemented.
Currently, it supports only two types: `logical` and `nautilus`.
There may be more types as we support more execution backends.
The optimizer knows that a generated plan is not executable as long as operators in the query plan still contain operators with the `logical` convention property.
This means that either more rules must be applied to the plan until all operators are non-logical, or the optimizer must discard the plan because the optimizer or execution engine is unable to process the plan.
Once we have a cost-based optimizer, it may stop searching if a plan's cost has not improved by more than a given threshold in the last planner iterations and all operators in that plan are non-logical.

```C++
/// Each Trait can be checked for satisfaction.
class Trait {
public:
    virtual ~Trait() {}
    virtual std::string getName() const = 0;
    virtual bool satisfies(const Trait &other) const = 0;
};

enum class ConventionType {
    LOGICAL,
    NAUTILUS
};

class ConventionTrait : public Trait {
public:
    ConventionTrait(ConventionType type) : type(type) {}

    std::string getName() const override { return "Convention"; }
    
    bool satisfies(const Trait &other) const override {
        const ConventionTrait* otherConv = dynamic_cast<const ConventionTrait*>(&other);
        if (!otherConv) return false;
        return this->type == otherConv->type;
    }

private:
    ConventionType type;
};
```

Examples for traits in NebulaStream are:
- _Data Format:_ Native, GZIP Compressed, SNAPPY Compressed
- _Windowing:_ count-based tumbling window, time-based tumbling window
- _Fault Tolerance Guarantees:_ at_least_once, exactly_once
- _Allowed Lateness (in window):_ Xs, Xms
- _Ordering (in window):_ unordered, ordered by X
- _Partitioning:_ not partitioned, hash partitioned, range partitioned
- _Data Input Mode:_ Batch Source, Unbounded Source
- _Placement:_ Placed at node X

## S2: Operator Representations
We differentiate between logical and physical by having a `LogicalOperator` to which we add gradually physical traits.
Once all operators are of the convention trait `Nautilus` the optimizer tries to match them to `ExecutableOperator`s.

The lowering does not happen with dedicated lowering phases, but is done by the same optimizer rules interface.
After exploration, logical operators with certain properties are mapped to 'non-logical' (for now Nautilus) operators.
We allow plugin developers to register new operator traits that can be used by their optimization rules.

**Rules Impl:**

```C++
struct Rule
{
    virtual ~Rule(const QueryPlan matchPattern) = default;
    virtual void onMatch(const RuleCall& call) = 0;
};

/// Invocation of a Rule with a set of Plans as arguments
struct RuleCall
{
    RuleCall(const std::vector<Plan>& matchedPlans);

    /// called by the rule whenever it finds a match
    void transformTo(const Plan &newPlan);
    
    /// List of operators that matched
    std::vector<Plan> matchedPlans;
}
```

Some rules need require statistics, the current load, or specific properties, based on what is available in our optimizer we choose register certain optimization rules either only on the global or the local optimizer.


All operators that need to be able to be part of the optimization process implement the following interface:
```C++
struct Operator {
    Operator(
        TraitSet traitSet,
        std::vector<std::shared_ptr<Operator>> inputs,
        Schema outputSchema);
    
    void operator<<(std::ostream &os) const;
    
    TraitSet traits;
    std::vector<std::shared_ptr<Operator>> inputs;
    Schema outputSchema;
};
```

All executable operators do have to inherit from both: NautilusOperator and LogicalOperator.
Executable operators have the convention 'Nautilus' since construction signaling to the optimizer that the operator can be executed.

Rules are applied in phases of the query optimizer.
What to do with them, i.e. how to apply them, is part of the optimization strategy used, i.e. Volcano.
For now, we simply apply all rules until they are exhausted.

## S3: Query Compiler

```C++
class QueryCompiler
{
public:
    QueryCompiler(std::shared_ptr<QueryCompilerOptions> options);

    void loadPhases();
    void compile(const std::shared_ptr<QueryCompilationRequest>& request);
        
    std::vector<Phase> getAllRegisteredPhases();
    std::vector<Rule> getAllRegisteredRules();
private:
    template <typename PhaseType>
    void addPhase();

    std::string explainPhases() const;
    
    std::unordered_map<QueryId, std::vector<QueryPlan>> queryIntermediateResults;
    std::vector<std::unique_ptr<Phase>> phases;
    std::shared_ptr<QueryCompilerOptions> options;
}
```

The implementation allows arbitrary pluggable query optimization passes.
Following our plugin approach, all phases during query optimization are internal or external plugins.
Optimization can be performed on multiple queries at once when we insert a query graph.
We allow re-triggering of optimization rules.
The query compiler is able to cache intermediate results between phases in `queryIntermediateResults`.
Any phase of the query compilation phase can be re-triggered.

A similar interface should and could be used for the global optimizer.

## S4: Query Registration

1. The AntlrQueryParser creates a QueryPlan using QueryPlanBuilder and maps it to LogicalOperators.
   The constructors of the logical operators are taken from the operator registry.
2. Nebuli contains the QueryOptimizer component, which is reused between the global optimizer and the local optimizer.
   For now, we suggest not distinguishing between QueryPlan and DecomposedQueryPlan, as they are too similar.
3. Nebuli serializes the plan into a SerializableQueryPlan.pb.
   The traits are modeled as separate classes. Serialization steps follow the same interface as optimization rules.
   We also send the original plan to allow local re-optimization.
   During this process, we gradually build a SerializableQueryPlan.pb.
4. Deserialization goes through the registry of logical operators that the operator applies to deserialize.
   Creates a QueryPlan object.
5. The QueryCompiler takes a `QueryCompilationRequest` containing the QueryPlan and additional QueryCompilations options and applies phases to plans.

# Proof of Concept
TBA

# Alternatives
TBA

# Appendix

[^1]: Apache Calcite: A Foundational Framework for Optimize Query Processing Over Heterogeneous Data Sources, Begoli et al.
[^2]: The cascades framework for query optimization, G. Graefe
[^3]: The Volcano Optimizer Generator: Extensibility and Efficient Search, G. Graefe
[^4]: Redbook, Chapter 7, M. Stonebraker, J. Hellerstein