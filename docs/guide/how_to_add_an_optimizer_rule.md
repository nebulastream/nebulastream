# How to add an Optimizer `Rule`

Query optimization in NebulaStream is rule-based.   
This includes the semantic analysis, the logical optimization, and the trait selection. 
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
# Load Plugin Registry Utils
include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

# 1) Register Plugin 
add_plugin_as_library(
        RedundantUnionRemoval               # Plugin Name 
        PlanRule                            # Registry Name
        RedundantUnionRemovalRulePlugin     # Plugin library Name
        RedundantUnionRemovalRule.cpp)      # List of source files


target_include_directories(RedundantUnionRemovalRulePlugin
        PUBLIC include
        PRIVATE .
)

# 2) Register Tests
if (NES_ENABLES_TESTS)
    add_nes_unit_test(RedundantUnionRemovalRuleTest RedundantUnionRemovalRuleTest.cpp)
    target_link_libraries(RedundantUnionRemovalRuleTest nes-query-optimizer nes-logical-operators nes-test-util nes-query-optimizer-test-utils-lib)
    target_include_directories(RedundantUnionRemovalRuleTest PRIVATE .)
endif ()
```

To activate the plugin, you must add the line `activate_optional_plugin("Rules/RedundantUnionRemovalRule" ON)` to `nes-plugins/CMakeLists.txt`.  


For a detailed explanation of the plugin system, CMake macros, and how registries work, see `guide/extensibility.md`.

## 3. Interface

Generally speaking, a rule can be any object that defines the following constant and method:

```cpp
// Human-readable name of rule 
static constexpr std::string_view NAME = "NameOfRule";

// Core logic of rule. Gets a LogicalPlan and returns a modified LogicalPlan
LogicalPlan apply(LogicalPlan queryPlan) const; 
```


Additionally and optionally, the following methods can be defined to overwrite the default behavior. 

```cpp
// returns a set of rules that must be applied before the given rule
std::set<std::type_index> needs() const;

// returns a set of rules that must be applied after the given rule
std::set<std::type_index> neededBy() const;

// relaxed version of needs(): returned rules must be applied before the given rule if the rules are registered
std::set<std::type_index> wants() const;

// relaxed version of neededBy(): returned rules must be applied after given rule if the rules are registered
std::set<std::type_index> wantedBy() const;

// equality operator
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

namespace NES
{


/// This pass removes redundant unions with only a single child.
class RedundantUnionRemovalRule
{
public:
    static constexpr std::string_view NAME = "RedundantUnionRemovalRule";

    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;
};

static_assert(RuleConcept<RedundantUnionRemovalRule, LogicalPlan>);
}
```

We first declare the required constant `NAME` and the required method `LogicalPlan apply(LogicalPlan)`. 
Then, because we want to control where the rule is placed within the dependency graph, we declare the methods
`std::set<std::type_index> needs() const` and 
`std::set<std::type_index> neededBy() const`. 
To assert that we correctly declared the rule, we assert that it follows the RuleConcept using the call
`static_assert(RuleConcept<RedundantUnionRemovalRule, LogicalPlan>);`.

Next, we define the actual behavior of the rule in `RedundantUnionRemovalRule.cpp`. 
To keep the example readable, we'll only focus on individual snippets. 

First, we define the `apply` method. 
We first ensure that the given plan only has one root operator.
Then we start a (yet-to-be-defined) recursive function call at that root operator, 
replace that root operator with the updated one,
and return the changed plan.

```cpp 
LogicalPlan RedundantUnionRemovalRule::apply(LogicalPlan queryPlan) const
{
    PRECONDITION(queryPlan.getRootOperators().size() == 1, "Query plan must have exactly one root operator");
    queryPlan = queryPlan.withRootOperators({recur(queryPlan.getRootOperators().front().withInferredSchema())});
    return queryPlan;
}
```

Next, we define what is happening in the recursion.
The algorithm works bottom up. Thus, we first continue the recursion for all child operators of the current operator. 
It will stop at Source operators because source operators are the only operators that do not have children. 
Then, if the current operator is a UNION operator with only one child, we return that child operator, thereby removing the unnecessary UNION operator. 
If the current operator is not a UNION operator or if it is but has more than one child, we leave it as is, 
but update its children with the recursively updated child operators. 
The whole function is defined within an anonymous namespace because there is no need to expose the function to other objects or classes 
by defining it in the class definition.

```cpp
namespace
{
LogicalOperator recur(const LogicalOperator& op)
{
    auto newChildren = op.getChildren() | std::views::transform(recur) | std::ranges::to<std::vector>();

    if (op.tryGetAs<UnionLogicalOperator>().has_value() && newChildren.size() == 1)
    {
        return newChildren.front();
    }
    return op.withChildren(std::move(newChildren));
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

To ensure that the NebulaStream optimizer is able to instantiate the newly created rule, we define the registration function.
While the `PlanRuleRegistryArguments` gives us access to multiple catalogs and also the optimizer configuration, 
we do not need either for the given rule, and thus can safely ignore it. 
The name of the registration must follow the pattern
`PlanRuleRegistryReturnType PlanRuleGeneratedRegistrar::Register<PLUGIN_NAME>PlanRule(PlanRuleRegistryArguments)`, 
where `<PLUGIN_NAME>` is equal to the plugin name defined in the plugin's CMakeLists.txt file. 

```cpp
PlanRuleRegistryReturnType PlanRuleGeneratedRegistrar::RegisterRedundantUnionRemovalPlanRule(PlanRuleRegistryArguments)
{
    return RedundantUnionRemovalRule{};
}
```

Last, it is good practice to define unit tests for the rule to ensure it works as expected. 
See `RedundantUnionRemovalRuleTest.cpp` for reference.    
