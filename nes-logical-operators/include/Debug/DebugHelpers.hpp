/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
        https://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once
#include <string>
#include <vector>
#include <Plans/LogicalPlan.hpp>
#include <Operators/LogicalOperator.hpp>

namespace NES::Debug
{

/// Debugger-only helpers. Not used by production code paths.
///
/// Intended usage: while paused at a breakpoint in GDB/CLion, evaluate
/// `NES::Debug::view(plan)` in the "Threads & Variables" expression box or the
/// GDB console to get a shallow, expandable operator tree.
///
/// The *View structs are deliberately tiny (one label string + children) so
/// the debugger renders them as a compact tree: operator -> children -> ...,
/// without the self/get()/impl indirection of the real operator objects.

/// One node of the shallow debug tree. `op` is the operator's short label and
/// `children` recurse into the subtree.
struct OperatorView
{
    std::string op;
    std::vector<OperatorView> children;
};

/// Root of the shallow debug tree for a whole plan.
struct PlanView
{
    std::string plan;
    std::vector<OperatorView> roots;
};

/// Build a shallow debug tree for a single operator (recurses into children).
[[nodiscard]] OperatorView view(const LogicalOperator& op);

/// Build a shallow debug tree for a whole plan.
[[nodiscard]] PlanView view(const LogicalPlan& plan);

}