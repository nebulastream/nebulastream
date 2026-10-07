# How to add an Optimizer `Rule`

Query optimization in NebulaStream is rule-based.   
This includes the semantic analysis, the logical optimization, and the physical optimization (trait selection).
The rules are defined in individual classes and loaded automatically if registered.

## 1. Overview

Rule plugins are implemented in the `nes-plugins/Rules/` directory. 
For each new rule, create a new directory, such as `nes-plugins/Rules/RedundantUnionRemovalRule/`.
This directory will contain the rule implementation.
Within that directory, you can structure your code as you wish. 
For our example, we use the following structure:

```
nes-plugins/
├── Rules/
│   ├── RedundantUnionRemovalRule/
│   │   ├── CMakeLists.txt
│   │   ├── RedundantUnionRemovalRule.cpp
│   │   ├── RedundantUnionRemovalRule.hpp
│   │   └── RedundantUnionRemovalRuleTest.cpp
│   └── ...
├── Sinks/
├── Sources/
└── ...
```

## 2. Plugin Registration 

Registering a rule plugin consists of:
1) The registration of the rule plugin
2) The registration of the defined unit tests
3) The activation of the rule plugin

The following is the commented `CMakeLists.txt` file for the RedundantUnionRemovalRule plugin. 

```cmake
# Load runtime registry utils
include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)

# 1) Build the plugin implementation library
add_library(RedundantUnionRemovalRulePlugin STATIC RedundantUnionRemovalRule.cpp)
target_link_libraries(RedundantUnionRemovalRulePlugin PRIVATE nes-query-optimizer)
target_include_directories(RedundantUnionRemovalRulePlugin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

# 2) Register the rule with the PlanRule registry
link_plugin_library(nes-query-optimizer RedundantUnionRemovalRulePlugin)
add_registry_entry(PlanRule RedundantUnionRemovalRule KEY RedundantUnionRemoval)

# 3) Register Tests
if (NES_ENABLES_TESTS)
    add_nes_unit_test(RedundantUnionRemovalRuleTest RedundantUnionRemovalRuleTest.cpp)
    target_link_libraries(RedundantUnionRemovalRuleTest nes-query-optimizer nes-logical-operators nes-test-util nes-query-optimizer-test-utils-lib)
    target_include_directories(RedundantUnionRemovalRuleTest PRIVATE .)
endif ()
```

The plugin name `RedundantUnionRemovalRule` must match the C++ type: the `PlanRule` registry derives
the header `RedundantUnionRemovalRule.hpp` and the entry expression `&RedundantUnionRemovalRule::create`
from `${PLUGIN_NAME}`. `KEY RedundantUnionRemoval` sets the registry lookup key under which the
optimizer instantiates the rule (it defaults to the plugin name if omitted).

To activate the plugin, you must add the line `add_plugin("Rules/RedundantUnionRemovalRule")` to `nes-plugins/CMakeLists.txt`.  


For a detailed explanation of the plugin system, CMake macros, and how registries work, see [guide/extensibility.md](extensibility.md).

## 3. Interface

Generally speaking, a rule can be any object that defines the following constant and method:

```cpp
/// Human-readable name of rule 
static constexpr std::string_view NAME = "NameOfRule";

/// Core logic of rule. Gets a LogicalPlan and returns a modified LogicalPlan
LogicalPlan apply(LogicalPlan queryPlan) const; 
```


Additionally and optionally, the following methods can be defined to overwrite the default behavior. 

```cpp
/// returns a set of rules that must be applied before the given rule
std::set<std::type_index> needs() const;

/// returns a set of rules that must be applied after the given rule
std::set<std::type_index> neededBy() const;

/// relaxed version of needs(): returned rules must be applied before the given rule if the rules are registered
std::set<std::type_index> wants() const;

/// relaxed version of neededBy(): returned rules must be applied after given rule if the rules are registered
std::set<std::type_index> wantedBy() const;

/// equality operator
bool operator==(const T& other) const
```

> Note: While the `Rule.hpp` defines rules using templates (`Rule<U>`) for future extensibility, 
> in our example we assume that rules always works on `LogicalPlan` (Rule<LogicalPlan>).


## 4. Rule Dependencies

To ensure the rules are applied in a valid order, rules can define hard and soft directed dependencies between each other.
Hard dependencies (`needs/neededBy`) define that a rule MUST run before another rule and will fail if that other rule is not available. 
E.g., if rule A needs rule B, rule A can only run after rule B has run. 
If rule B is not run (e.g., because it is not registered), an error is thrown when the rule sequence is generated.
Soft dependencies (`wants/wantedBy`) define that a rule must run before another rule, but the dependency is ignored if that other rule is not available.
E.g., if rule A wants rule B, rule A must run after rule B if rule B is also registered, 
but if rule B is not registered, A can still run, and the `wants` dependency on B is ignored.    
These dependencies are then used to create a directed acyclic graph.
If the graph is cycle-free, a topological sort is used to generate a valid and ordered rule sequence.
If the graph has cycles, an error is thrown.

To make dependency definition easier, a set of barrier rules is defined.
These barrier rules themselves do not change the query plan in any way,
but only work as a clear barrier between different phases.
At the time of writing, the phases/barriers are defined as follows:

```
{Rules that ensure the semantic correctness of the plan}
    --> BARRIER: SemanticAnalysisBarrier -->
{Rules that change plans in fundamental ways, by reordering, removing, or adding operators}
    --> BARRIER: FixedPlanStructureBarrier -->
{Rules that only change the behavior of individual operators, e.g. by adding Traits}
```


## 5. Example Implementation

Let's look at the `RedundantUnionRemovalRule` as an example implementation. 
The core idea of the rule is that we want to remove unnecessary UNION operators.
UNION operators are unnecessary if they only union the results of a single child operator. 

First, we define the header `RedundantUnionRemovalRule.hpp`.

```cpp
#pragma once

#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{


/// This pass removes redundant unions with only a single child.
class RedundantUnionRemovalRule
{
public:
    static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments arguments);
    static constexpr std::string_view NAME = "RedundantUnionRemovalRule";

    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;
};

static_assert(RuleConcept<RedundantUnionRemovalRule, LogicalPlan>);
}
```

We first declare the static `create` function through which the registry instantiates the rule (see the
end of this section), the required constant `NAME`, and the required method `LogicalPlan apply(LogicalPlan)`. 
Then, because we want to control where the rule is placed within the dependency graph, we declare the methods
`std::set<std::type_index> needs() const` and 
`std::set<std::type_index> neededBy() const`. 
To assert that we correctly declared the rule, we assert that it follows the RuleConcept using the call
`static_assert(RuleConcept<RedundantUnionRemovalRule, LogicalPlan>);`.

Next, we define the actual behavior of the rule in `RedundantUnionRemovalRule.cpp`. 
To keep the example readable, we'll only focus on individual snippets. 

Traversing and rebuilding a `LogicalPlan` by hand is easy to get wrong for plans with multiple root
operators (e.g. several sinks sharing part of a subplan), since a shared operator must then be visited and
rebuilt exactly once, not once per parent. Rather than writing that traversal ourselves, we use the generic
`PlanVisitor` utility (`Rules/PlanVisitor.hpp`), which walks the plan bottom-up (and, if needed, top-down first)
and calls back into our rule for each operator.

First, we define the `apply` method. 
We construct a `PlanVisitor<>` — using the default `OperatorContext`, `DownContext`, and `UpContext` of
`std::monostate`, since this rule does not need to thread any information between operators — and hand it our
bottom-up callback `redundantUnionRemoval`. `PlanVisitor` takes care of visiting every operator exactly once and
rebuilding the plan for us, so we no longer need to assert that the plan has a single root or manage the
recursion ourselves.

```cpp 
LogicalPlan RedundantUnionRemovalRule::apply(LogicalPlan queryPlan) const
{
    PlanVisitor<> visitor{redundantUnionRemoval};
    return visitor.apply(std::move(queryPlan));
}
```

Next, we define what happens on the bottom-up pass.
`PlanVisitor` calls `redundantUnionRemoval` once per operator, always after all of that operator's children have
already been rebuilt, passing in the operator itself and its (already rebuilt) children.
If the current operator is a UNION operator with only one child, we return that child operator, thereby removing
the unnecessary UNION operator. 
If the current operator is not a UNION operator or if it is but has more than one child, we leave it as is, 
but rebuild it with the already-rebuilt children.
Because this rule needs neither the top-down `OperatorContext` nor per-child `UpContext`, we use `PlanVisitor`'s
reduced-arity `FunctionUpAlt3` signature — `(LogicalOperator op, std::vector<LogicalOperator> children) -> UpResult` —
instead of the full `FunctionUp` signature; see `PlanVisitor.hpp` for the other available callback signatures.
The whole function is defined within an anonymous namespace because there is no need to expose the function to
other objects or classes by defining it in the class definition.

```cpp
namespace
{
PlanVisitor<>::UpResult redundantUnionRemoval(const LogicalOperator& op, std::vector<LogicalOperator> children)
{
    if (op.tryGetAs<UnionLogicalOperator>().has_value() && children.size() == 1)
    {
        return children.front();
    }
    return op.withChildren(std::move(children));
}
}
```

We want the rule to run after the semantic analysis is performed, but before we guarantee that the LogicalPlan is fixed.
Thus, we define a `needs` dependency on `SemanticAnalysisBarrier` and a `neededBy` dependency on `FixedPlanStructureBarrier`.

```cpp
std::set<std::type_index> RedundantUnionRemovalRule::needs() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

std::set<std::type_index> RedundantUnionRemovalRule::neededBy() const
{
    return {typeid(FixedPlanStructureBarrier)};
}
```

To ensure that the NebulaStream optimizer is able to instantiate the newly created rule, we define the static `create`
function declared in the header. The `PlanRule` registry's entry expression is `&<PLUGIN_NAME>::create`, so every rule
plugin must expose a static member with the signature
`static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments)`.
While the `PlanRuleRegistryArguments` gives us access to multiple catalogs and also the optimizer configuration, 
we do not need either for the given rule, and thus can safely ignore it. 

```cpp
PlanRuleRegistryReturnType RedundantUnionRemovalRule::create(PlanRuleRegistryArguments)
{
    return RedundantUnionRemovalRule{};
}
```

Last, it is good practice to define unit tests for the rule to ensure it works as expected. 
See `RedundantUnionRemovalRuleTest.cpp` for reference.    
