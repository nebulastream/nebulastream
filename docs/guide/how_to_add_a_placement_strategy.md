# How to add a `Placement Strategy`

Operator placement decides on which worker each logical operator runs.
It is separate from source and sink placement: sources and sinks already carry their configured `Host`, while a placement strategy assigns a `PlacementTrait` to every logical operator in between.
Downstream distributed-planning code consumes these traits to create per-worker fragments and network edges.

`BottomUpOperatorPlacer` is the current placement implementation and the best reference when adding a new placement strategy.

## 1. Overview

Placement code lives in `nes-query-optimizer`:

```text
nes-query-optimizer/
├── include/Placement/
│   ├── BottomUpPlacement.hpp
│   ├── QueryDecomposition.hpp
│   └── MyPlacement.hpp
└── src/Placement/
    ├── BottomUpPlacement.cpp
    ├── QueryDecomposition.cpp
    ├── MyPlacement.cpp
    └── CMakeLists.txt
```

A placement strategy should follow the shape of `BottomUpOperatorPlacer`:

```c++
class MyOperatorPlacer final
{
    SharedPtr<const WorkerCatalog> workerCatalog;

public:
    explicit MyOperatorPlacer(SharedPtr<const WorkerCatalog> workerCatalog) : workerCatalog(std::move(workerCatalog)) { }

    void apply(LogicalPlan& logicalPlan);
};
```

`apply` mutates the `LogicalPlan` by replacing it with an equivalent plan whose operators all have a `PlacementTrait`.
The placement strategy should not insert network operators itself; that happens after placement.

## 2. Dependencies & CMake

Add the new implementation file to `nes-query-optimizer/src/Placement/CMakeLists.txt`:

```cmake
add_source_files(nes-query-optimizer
        BottomUpPlacement.cpp
        QueryDecomposition.cpp
        MyPlacement.cpp
)
```

The placement strategy becomes part of the `nes-query-optimizer` library.
There is currently no placement-strategy plugin registry, so a new strategy must be wired into `OperatorPlacer` explicitly.
If the strategy needs an additional dependency, add the `find_package` call and `target_link_libraries` entry in `nes-query-optimizer/CMakeLists.txt`.
`BottomUpOperatorPlacer` is an example: it uses HiGHS and `nes-query-optimizer` links `highs::highs` privately.

## 3. Selection & Configuration

`OperatorPlacer::place` is the placement integration point.
It currently selects the placement strategy by directly constructing `BottomUpOperatorPlacer`.

For a one-off replacement, include the new placement header in `nes-query-optimizer/src/Phases/OperatorPlacer.cpp` and call the new placer where `BottomUpOperatorPlacer` is invoked.

For a selectable strategy, add an enum and an `EnumOption` to `nes-query-optimizer/interface/QueryOptimizerConfiguration.hpp`:

```c++
enum class OperatorPlacementStrategy : uint8_t
{
    BOTTOM_UP,
    MY_STRATEGY
};

EnumOption<OperatorPlacementStrategy> placementStrategy
    = {"placement_strategy",
       OperatorPlacementStrategy::BOTTOM_UP,
       "Operator placement strategy [BOTTOM_UP|MY_STRATEGY]."};

std::vector<BaseOption*> getOptions() override { return {&joinStrategy, &placementStrategy, &network}; }:
```

Configuration enum values are parsed by name, so the example strategy can be selected with `placement_strategy=MY_STRATEGY`.

Then select the strategy in `OperatorPlacer::place` before query decomposition:

```c++
switch (defaultQueryOptimization.placementStrategy)
{
    case OperatorPlacementStrategy::BOTTOM_UP:
        BottomUpOperatorPlacer(copyPtr(workerCatalog)).apply(plan);
        break;
    case OperatorPlacementStrategy::MY_STRATEGY:
        MyOperatorPlacer(copyPtr(workerCatalog)).apply(plan);
        break;
}
```

The strategy must run at the placement integration point, because downstream distributed-planning code expects every operator to have a `PlacementTrait`.

## 4. Implementation

A placement strategy receives:

- the `LogicalPlan` handed to placement
- the `WorkerCatalog`
- the current `NetworkTopology` via `workerCatalog->getTopology()`
- worker capacities via `workerCatalog->getWorker(host).value().maxOperators`
- fixed source hosts via `SourceDescriptorLogicalOperator::getSourceDescriptor().getHost()`
- fixed sink hosts via `SinkLogicalOperator::getSinkDescriptor()->getHost()`

A placement strategy must produce:

- exactly one worker assignment for every logical operator in the plan
- a `PlacementTrait` on every logical operator
- a plan with the same logical structure, enriched with placement traits, so downstream distributed planning can process it

Recommended behavior:

- Validate that all source and sink hosts exist in the topology.
- Validate that the topology connects every placed source to the placed sink.
- Keep source operators on their source descriptor host.
- Keep the sink operator on its sink descriptor host.
- Respect `maxOperators` if the strategy models worker capacity.
- Throw `PlacementFailure` for user-facing infeasibility.
- Use `PRECONDITION` or `INVARIANT` for internal bugs.

`BottomUpOperatorPlacer` follows by building a placement map from `OperatorId` to `NetworkTopology::NodeId`, then recursively attaching `PlacementTrait` to every operator:

```c++
LogicalOperator addPlacementTrait(const LogicalOperator& op, const std::unordered_map<OperatorId, NetworkTopology::NodeId>& placement)
{
    auto oldTraitSet = op.getTraitSet();
    USED_IN_DEBUG auto addedTrait = oldTraitSet.tryInsert(PlacementTrait(placement.at(op.getId())));
    INVARIANT(addedTrait, "There should not have been a placement trait");

    return op.withTraitSet(oldTraitSet)
        .withChildren(
            op.getChildren()
            | std::views::transform(
                [&placement](const LogicalOperator& child) -> LogicalOperator { return addPlacementTrait(child, placement); })
            | std::ranges::to<std::vector>());
}
```

Use the same immutable-plan style: create updated operators with `withTraitSet` and `withChildren`, then replace the plan root:

```c++
logicalPlan = LogicalPlan(logicalPlan.getQueryId(), {addPlacementTrait(logicalPlan.getRootOperators().front(), placement)});
```

### Example Strategy Shape

A simple strategy can be structured as:

1. Read a topology snapshot from the worker catalog.
2. Validate source and sink hosts.
3. Iterate over the plan with `BFSRange(logicalPlan.getRootOperators().front())`.
4. Choose a `NetworkTopology::NodeId` for each operator.
5. Attach placement traits recursively.

## 5. Testing

Test both the strategy-specific placement decisions and the distributed plan produced after placement.
Useful existing references are in `nes-frontend/tests/DistributedPlanningTest.cpp`.

