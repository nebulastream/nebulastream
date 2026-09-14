# How to add an `Operator`

Operators are the nodes of a query plan: selection (`WHERE`), projection, joins, windowed
aggregations, watermark assignment, and so on. Unlike sources and sinks, operators are not
plugins living in `nes-plugins`. They are core components split across three components that
mirror the phases a query goes through:

- `nes-logical-operators`: the **logical operator**, used during parsing, binding and
  optimization. It carries no execution logic, only schema/type information and structure.
- `nes-physical-operators`: the **physical operator**, the actual per-record execution logic
  run by the query engine.
- `nes-query-compiler`: the **lowering rule** that translates one logical operator into a
  subgraph of physical operators.

This guide walks through all three using `SelectionLogicalOperator`/`SelectionPhysicalOperator`
(the operator behind SQL `WHERE`) as a running example, because it is the simplest operator that
still touches every piece of the pipeline. More complex operators (joins, windowed aggregations)
follow the same shape but additionally carry an `OperatorHandler` for state that survives across
records/buffers (see `HJOperatorHandler`/`WindowBasedOperatorHandler`).

## 1. Overview

```
nes-logical-operators/
├── include/Operators/
│   ├── LogicalOperator.hpp          # type-erased LogicalOperator wrapper + concept machinery
│   ├── SelectionLogicalOperator.hpp
│   └── ...
├── src/Operators/
│   ├── SelectionLogicalOperator.cpp
│   └── CMakeLists.txt               # add_unreflection_entry(...) lives here
├── registry/include/
│   └── LogicalOperatorRegistry.hpp
└── ...

nes-physical-operators/
├── include/
│   ├── PhysicalOperator.hpp          # PhysicalOperatorConcept + type-erased wrapper
│   └── SelectionPhysicalOperator.hpp
├── src/
│   └── SelectionPhysicalOperator.cpp
└── ...

nes-query-compiler/
├── private/LoweringRules/
│   ├── AbstractLoweringRule.hpp
│   └── LowerToPhysical/LowerToPhysicalSelection.hpp
├── src/LoweringRules/LowerToPhysical/
│   ├── LowerToPhysicalSelection.cpp
│   └── CMakeLists.txt                # add_registry_entry(LoweringRule ...) lives here
└── registry/include/LoweringRuleRegistry.hpp
```

A new operator needs a class in each of the first two components, plus a lowering rule in the
third, plus two one-line CMake registrations. `nes-query-optimizer` changes are only needed for
an optimization specific to the operator (see [6. Optimizer integration](#6-optimizer-integration)).

## 2. The Logical Operator

Logical operators are not implemented via inheritance from a common base class; instead, each
operator type is a plain class that satisfies the `LogicalOperatorConcept` (defined in
`nes-logical-operators/include/Operators/LogicalOperatorFwd.hpp`) and is wrapped in
`TypedLogicalOperator<T>` for type erasure. `LogicalOperator` is simply
`TypedLogicalOperator<detail::ErasedLogicalOperator>`. The concept requires:

```c++
{ thisOperator.explain(verbosity, operatorId) } -> std::convertible_to<std::string>;
{ thisOperator.getChildren() } -> std::convertible_to<std::vector<LogicalOperator>>;
{ thisOperator.withChildrenUnsafe(children) } -> std::convertible_to<T>;
{ thisOperator.withChildren(children) } -> std::convertible_to<T>;
{ thisOperator.withTraitSet(traitSet) } -> std::convertible_to<T>;
{ thisOperator == rhs } -> std::convertible_to<bool>;
{ thisOperator.getName() } noexcept -> std::convertible_to<std::string_view>;
{ thisOperator.getTraitSet() } -> std::convertible_to<TraitSet>;
{ thisOperator.getOutputSchema() } -> std::same_as<Schema<Field, Unordered>>;
{ thisOperator.withInferredSchema() } -> std::convertible_to<T>;
```

`SelectionLogicalOperator` (`nes-logical-operators/include/Operators/SelectionLogicalOperator.hpp`)
implements this for the `WHERE` predicate:

```c++
class SelectionLogicalOperator : public ManagedByOperator
{
public:
    explicit SelectionLogicalOperator(WeakLogicalOperator self, LogicalFunction predicate);
    SelectionLogicalOperator(WeakLogicalOperator self, LogicalOperator child, LogicalFunction predicate);

    static TypedLogicalOperator<SelectionLogicalOperator> create(LogicalFunction predicate);
    static TypedLogicalOperator<SelectionLogicalOperator> create(LogicalOperator child, LogicalFunction predicate);

    [[nodiscard]] LogicalFunction getPredicate() const;
    [[nodiscard]] bool operator==(const SelectionLogicalOperator& rhs) const;
    [[nodiscard]] SelectionLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;
    [[nodiscard]] SelectionLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] SelectionLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] SelectionLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "Selection";
    std::optional<LogicalOperator> child;
    LogicalFunction predicate;
    void inferLocalSchema();
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
    TraitSet traitSet;
};
```

A few things worth calling out from this and from `SelectionLogicalOperator.cpp`:

- **Every constructor takes a `WeakLogicalOperator self` first.** The type-erasure wrapper
  (`detail::OperatorModel<T>` in `LogicalOperator.hpp`) constructs the operator by passing its
  own weak self-reference as the first argument, then the remaining constructor arguments.
  `ManagedByOperator` stores it.
- **`create()` is a static factory, not the constructor**, and is what other code (e.g.
  `LogicalPlanBuilder`) calls: `SelectionLogicalOperator::create(predicate)` /
  `SelectionLogicalOperator::create(child, predicate)`. `TypedLogicalOperator<T>`'s templated
  constructor perfect-forwards these arguments after `self`.
- **Schema inference is local and lazy.** `inferLocalSchema()` (private) reads the child's
  already-inferred output schema, resolves the operator's own `LogicalFunction`/output types
  against it (`predicate.withInferredDataType(inputSchema)` here), and throws
  `CannotInferSchema` if the operator's contract is violated (selection requires a `BOOLEAN`
  predicate). `withInferredSchema()` is the public, immutable version: it recurses into the
  child first (`copy.child = copy.child->withInferredSchema()`), then calls
  `inferLocalSchema()`. Because operators are otherwise immutable value types (every "with"
  method returns a modified copy), schema inference has to be re-run whenever children change.
  This is why `withChildren` re-infers while `withChildrenUnsafe` does not (used when the schema
  is known to be unaffected, e.g. structural plan rewrites).
- **`getOutputSchema()` binds the locally-stored, unbound schema back to `self`** via
  `NES::bindToOperator(self.lock(), outputSchema.value())`. Schemas are stored unbound
  internally and bound to the owning operator instance on read.
- **`explain()` has a `Debug` verbosity branch** that includes the operator id, predicate, and
  trait set, and a terser default branch for normal plan printing, used by `EXPLAIN` output and
  log messages.
- **`operator==` is structural** (compares `predicate`, `outputSchema`, `traitSet`), and a
  matching `std::hash<NES::SelectionLogicalOperator>` specialization is required alongside it.
  It is declared as a `friend struct std::hash<...>` in the header and defined at the bottom of
  the `.cpp` file, hashing whichever fields participate in equality.

### Serialization

Every logical operator also needs `Reflector`/`Unreflector` specializations for
`TypedLogicalOperator<T>`, so it can be sent across the wire and reconstructed by another worker.
See [Serialization](../technical/serialization.md) for how reflection works in general.
`SelectionLogicalOperator`'s specializations (`SelectionLogicalOperator.hpp`) are a minimal
example, reflecting just the predicate (children are handled separately by the plan).

## 3. Registering the Logical Operator

Add your `.cpp` file to the component's `add_source_files(...)` call
(`nes-logical-operators/src/Operators/CMakeLists.txt`), then register it for deserialization:

```cmake
add_unreflection_entry(LogicalOperator Selection)
```

This macro (`cmake/UnreflectionRegistrationUtil.cmake`) generates a small glue translation unit
that registers `TypedLogicalOperator<SelectionLogicalOperator>` under the key `"Selection"` in
the `LogicalOperatorUnreflectionRegistry`, so a reflected plan received over the network can be
turned back into the operator type. The plugin name (`Selection`) must match the operator's
`getName()`. That string is the wire-format `type` field, and it's also how the physical
lowering step in the next section finds the operator by name. `add_registry_entry`/
`add_unreflection_entry` calls can run before their registry exists (top-level components
configure alphabetically); they get deferred automatically regardless of configuration order.

There is a separate, currently-unused `LogicalOperatorRegistry` (a *factory*-by-name registry,
distinct from the unreflection registry above) declared in the same `CMakeLists.txt`. As of this
writing no operator registers an entry for it (`add_registry_entry(LogicalOperator <name>)`
would be the call). It exists as a construction surface for future name-based operator creation,
and is not required for a new operator to work.

## 4. The Physical Operator

Physical operators implement `PhysicalOperatorConcept`
(`nes-physical-operators/include/PhysicalOperator.hpp`):

```c++
struct PhysicalOperatorConcept
{
    virtual ~PhysicalOperatorConcept() = default;

    [[nodiscard]] virtual std::optional<PhysicalOperator> getChild() const = 0;
    virtual void setChild(PhysicalOperator child) = 0;

    virtual void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const;
    virtual void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    virtual void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    virtual void terminate(ExecutionContext& executionCtx) const;
    virtual void execute(ExecutionContext& executionCtx, Record& record) const;

    const OperatorId id = INVALID_OPERATOR_ID;
protected:
    void setupChild(...) const;   // and openChild/closeChild/executeChild/terminateChild
};
```

`setup`/`open`/`close`/`terminate` have empty default implementations; only the ones needed are
overridden. `execute` runs per-record and is the one virtually every operator implements.
`SelectionPhysicalOperator` (`nes-physical-operators/include/SelectionPhysicalOperator.hpp` /
`src/SelectionPhysicalOperator.cpp`) only needs `execute`, `getChild`, and `setChild`:

```c++
class SelectionPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    explicit SelectionPhysicalOperator(PhysicalFunction function) : function(std::move(function)) { };
    void execute(ExecutionContext& ctx, Record& record) const override;
    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    const PhysicalFunction function;
    std::optional<PhysicalOperator> child;
};

void SelectionPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// evaluate function and call child operator if function is valid
    if (function.execute(record, ctx.pipelineMemoryProvider.arena))
    {
        executeChild(ctx, record);
    }
}
```

Physical operators form a **pull-less, push-down chain within one pipeline**: `execute` decides
whether/how to call `executeChild(ctx, record)` to hand the (possibly transformed) record to the
next operator in the same pipeline. A selection either forwards the record unchanged or drops it.
A map operator (see `MapPhysicalOperator`) would mutate fields on `record` before forwarding.
Window/join build operators instead buffer the record into a slice and don't call `executeChild`
at all; that only happens later, from the probe side, when a window/join is triggered. Use
`setup`/`open`/`close`/`terminate` for resources scoped to the query, a buffer, or the pipeline
run respectively. See [How to add a Source](how_to_add_a_source.md) and
[How to add a Sink](how_to_add_a_sink.md) for the equivalent lifecycle there.

There is no CMake registry step for physical operators themselves; they aren't looked up by name
at runtime. They're only ever constructed directly, by the lowering rule described next.

## 5. The Lowering Rule

A lowering rule implements `AbstractLoweringRule`
(`nes-query-compiler/private/LoweringRules/AbstractLoweringRule.hpp`):

```c++
struct LoweringRuleResultSubgraph
{
    using SubGraphRoot = std::shared_ptr<PhysicalOperatorWrapper>;
    using SubGraphLeaves = std::vector<std::shared_ptr<PhysicalOperatorWrapper>>;
    SubGraphRoot root;
    SubGraphLeaves leaves;
};

struct AbstractLoweringRule
{
    virtual LoweringRuleResultSubgraph apply(LogicalOperator logicalOperator) = 0;
    virtual ~AbstractLoweringRule() = default;
};
```

`apply` takes one logical operator and returns a physical *subgraph* wrapped in
`PhysicalOperatorWrapper`, usually a single node for a simple operator like selection, but a
lowering rule can expand one logical operator into several physical operators (e.g. a windowed
aggregation lowers into separate build/probe physical operators with an `OperatorHandler` between
them). `LowerToPhysicalSelection`
(`nes-query-compiler/private/LoweringRules/LowerToPhysical/LowerToPhysicalSelection.hpp` /
`src/LoweringRules/LowerToPhysical/LowerToPhysicalSelection.cpp`) is the minimal, single-node
case:

```c++
struct LowerToPhysicalSelection : AbstractLoweringRule
{
    explicit LowerToPhysicalSelection(QueryExecutionConfiguration conf) : conf(std::move(conf)) { }
    LoweringRuleResultSubgraph apply(LogicalOperator logicalOperator) override;
private:
    QueryExecutionConfiguration conf;
};

LoweringRuleResultSubgraph LowerToPhysicalSelection::apply(LogicalOperator logicalOperator)
{
    const auto selection = logicalOperator.getAs<SelectionLogicalOperator>();
    const auto function = selection->getPredicate();
    const auto func = QueryCompilation::FunctionProvider::lowerFunction(
        function, *selection->getChild()->getTraitSet().get<FieldMappingTrait>());
    const auto traitSet = logicalOperator.getTraitSet();

    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto inputSchema = createPhysicalOutputSchema(selection->getChild()->getTraitSet());

    auto physicalOperator = SelectionPhysicalOperator(func);
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator, inputSchema, outputSchema, memoryLayoutType, memoryLayoutType,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leaves(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leaves = {leaves}};
}
```

Every constructor takes a `QueryExecutionConfiguration`. Lowering rules are constructed once per
query compilation, not once globally, so runtime configuration (e.g. join strategy) can flow into
the decision of which physical operators to produce. `logicalOperator.getAs<T>()` is the
type-erasure downcast (throws if the operator isn't actually a `T`; use `tryGetAs<T>()` if the
cast can legitimately fail).

Note the two `FieldMappingTrait` and `MemoryLayoutTypeTrait` lookups: these traits are attached
to every operator generically, before lowering, by static optimizer rules in
`nes-query-optimizer` (`DecideFieldMappings`, `DecideMemoryLayoutRule`, see
[6. Optimizer integration](#6-optimizer-integration)). A lowering rule reads them off
`getTraitSet()`; it does not need to compute them itself, for selection or for most other
non-windowing operators.

`PhysicalOperatorWrapper` carries the metadata the query compiler needs to stitch operators into
pipelines and is discarded once compilation is done. The physical operator itself only knows
about its immediate child. Use `PipelineLocation::SCAN`/`EMIT`/`INTERMEDIATE` depending on
whether the operator opens, closes, or sits in the middle of a pipeline (only sources/sinks use
`SCAN`/`EMIT`; nearly everything else, selection included, is `INTERMEDIATE`). Operators needing
cross-buffer state pass an `OperatorHandlerId`/`OperatorHandler` via the constructor overload that
accepts them (see `LowerToPhysicalWindowedAggregation.cpp` for that shape).

### Registering the lowering rule

```cmake
add_registry_entry(LoweringRule Selection)
```

in `nes-query-compiler/src/LoweringRules/LowerToPhysical/CMakeLists.txt`, next to the
`LowerToPhysicalSelection.cpp` entry in `add_source_files(...)`. The registry itself is declared
once in `nes-query-compiler/CMakeLists.txt`:

```cmake
create_runtime_registry(LoweringRule nes-query-compiler
        ENTRY_TEMPLATE "makeLoweringRule<LowerToPhysical${PLUGIN_NAME}>()"
        HEADER_TEMPLATE "LowerToPhysical${PLUGIN_NAME}.hpp"
        INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/private)
```

so `add_registry_entry(LoweringRule Selection)` expands to registering
`makeLoweringRule<LowerToPhysicalSelection>()` under the key `"Selection"`. There's no
hand-written `RegisterXxx` function to write, unlike the source/sink plugin registries. At
compile time, `LowerToPhysicalOperators.cpp` resolves the rule for a given logical operator by
looking up `LoweringRuleRegistry::instance().find(std::string(logicalOperator.getName()))`. This
is why the plugin name passed to `add_registry_entry` and the unreflection entry in
[3. Registering the Logical Operator](#3-registering-the-logical-operator) must both match the
operator's `getName()` string. `Join` is the one hard-coded exception: it dispatches on a
`JoinImplementationTypeTrait` to pick between the `HashJoin`/`NLJoin` registry entries instead of
using its own `getName()` directly. This is irrelevant unless you're adding a new join strategy.

## 6. Optimizer integration

Most static optimizer rules in `nes-query-optimizer/src/Rules/Static/` (schema/type inference,
`DecideFieldMappings`, `DecideFieldOrder`, `DecideMemoryLayoutRule`, `OriginIdInferenceRule`)
operate generically through the `LogicalOperatorConcept` interface (`getChildren`,
`withChildren`, `getOutputSchema`, trait accessors) and require no changes for a new operator.
This is why `LowerToPhysicalSelection` above could just read `FieldMappingTrait` and
`MemoryLayoutTypeTrait` off the trait set without any Selection-specific optimizer code.

Some rules are written against specific operator types because they encode an optimization or
correctness constraint that only makes sense for that operator, e.g.
`PredicatePushdownRule`/`ProjectionPushdownRule`/`WatermarkAssignerPushdownRule` all special-case
`SelectionLogicalOperator` (or the operator being pushed) to move it around the plan tree.
`nes-query-optimizer` only needs changes if the new operator should participate in an existing
rule like this, or needs a new rule of its own (e.g. a pushdown/pullup opportunity specific to
it). A brand-new operator with no such optimization compiles and executes correctly without any
`nes-query-optimizer` changes: the trait/schema propagation described above is enough.

## 7. Wiring it into SQL

`LogicalPlanBuilder` (`nes-logical-operators/include/Plans/LogicalPlanBuilder.hpp`) is the narrow
API that constructs and attaches logical operators to a `LogicalPlan`. The ANTLR-generated parser
in `nes-sql-parser` walks the parsed query and calls it; for selection, `AntlrSQLQueryPlanCreator.cpp`
does this for every collected `WHERE`/`HAVING` clause:

```c++
for (auto whereExpr = helpers.top().getWhereClauses().rbegin(); whereExpr != helpers.top().getWhereClauses().rend(); ++whereExpr)
{
    queryPlan = LogicalPlanBuilder::addSelection(std::move(*whereExpr), queryPlan);
}
```

and `LogicalPlanBuilder::addSelection` itself is a thin wrapper that attaches the operator to the
plan:

```c++
LogicalPlan LogicalPlanBuilder::addSelection(LogicalFunction selectionFunction, const LogicalPlan& queryPlan)
{
    return promoteOperatorToRoot(queryPlan, SelectionLogicalOperator::create(std::move(selectionFunction)));
}
```

If a new operator is exposed through SQL syntax that already exists (an existing clause, an
existing function-call position, and so on), this is usually all that's needed: a new
`LogicalPlanBuilder::addYourOperator(...)` method following the pattern above, called from the
visitor method in `AntlrSQLQueryPlanCreator.cpp` that already handles that clause.

A new operator that needs genuinely new SQL syntax (a new keyword or clause with no existing
equivalent) additionally needs a grammar change in `nes-sql-parser/AntlrSQL.g4` and a new
`visit...` override in `AntlrSQLQueryPlanCreator.cpp` to call your new `LogicalPlanBuilder`
method. Writing an ANTLR grammar rule is its own topic and outside what this guide covers; see
the [ANTLR4 reference](https://github.com/antlr/antlr4/blob/master/doc/index.md) and the existing
rules in `AntlrSQL.g4` for the surrounding syntax.

## 8. Testing

See [Testing](testing.md) for the full test taxonomy.

Unit tests: place logical-operator tests under `nes-logical-operators/tests/` (see
`JoinLogicalOperatorTest.cpp` for a logical-operator example that builds a plan, serializes and
deserializes it via `QueryPlanSerializationUtil`, and checks round-tripping) and
physical-operator tests under `nes-physical-operators/tests/` (see
`EmitPhysicalOperatorTest.cpp`). Register new test files in the component's
`tests/CMakeLists.txt`. Favor unit tests for schema-inference edge cases, serialization
round-trips, and any algorithmic logic in the physical operator's `execute` that a SQL query
cannot pin down precisely.

Systests prove the operator is reachable via SQL and produces correct output end-to-end.
Existing selection systests live under `nes-systests/operator/selection/*.test`
(`SelectionPredicates.test`, `Selection_NotEquals.test`); follow that
`nes-systests/operator/<operator-name>/` convention for a new operator.
