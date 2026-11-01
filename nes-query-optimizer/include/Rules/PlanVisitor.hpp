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

#include <cstdint>
#include <functional>
#include <ranges>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <Operators/LogicalOperatorFwd.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>

/**
 * @file PlanVisitor.hpp
 * @brief Generic two-pass traversal/rewrite utility for LogicalPlan DAGs.
 *
 * PlanVisitor<OperatorContext, DownContext, UpContext> walks a LogicalPlan first top-down
 * (sinks to sources) and then bottom-up (sources to sinks), threading user-defined context
 * through the traversal. It is used to implement rules that need to accumulate information
 * while descending the plan and then rebuild operators using that information while ascending.
 *
 * A plan may have multiple root operators (e.g. several sinks sharing part of a subplan),
 * so the traversal handles a DAG, not just a tree: each operator is visited exactly once
 * per pass, even if it is shared by multiple parents. The top-down pass only visits a node
 * once all its parents have propagated their DownContext to it, and the bottom-up pass only
 * rebuilds a node once all its children have been rebuilt; the resulting operator is then
 * reused, not duplicated, by every parent that shares it.
 *
 * ## Callbacks
 * - `FunctionDown`: `(LogicalOperator op, std::vector<DownContext> downContexts) -> DownResult`
 *   Computes op's own OperatorContext (later handed to FunctionUp) from the DownContexts
 *   propagated by its parents (one entry per parent edge, empty for a root), and a per-child
 *   DownContext map to propagate further down — letting different children of the same
 *   operator receive different contexts.
 * - `FunctionUp`: `(LogicalOperator op, std::vector<LogicalOperator> newChildren, OperatorContext context,
 *   std::unordered_map<LogicalOperator, UpContext> upContexts) -> UpResult`
 *   Rebuilds/rewrites op using its already-rebuilt children, the OperatorContext computed for
 *   it during the top-down pass, and the UpContext each child produced (keyed by the rebuilt
 *   child), then returns the new operator together with the UpContext to forward to its own
 *   parent(s).
 *
 * ## Alternative callback signatures
 * The constructors also accept reduced-arity variants for rules that do not need the full
 * signature, so callers do not have to accept and ignore parameters they never use:
 * - `FunctionDownAlt`: `(LogicalOperator op) -> DownResult`
 *   Drops the DownContexts parameter, for rules that only depend on the operator itself.
 * - `FunctionUpAlt1`: `(LogicalOperator op, std::vector<LogicalOperator> newChildren, OperatorContext context) -> UpResult`
 *   Drops the UpContexts map, for rules that do not need per-child UpContext.
 * - `FunctionUpAlt2`: `(LogicalOperator op, std::vector<LogicalOperator> newChildren,
 *   std::unordered_map<LogicalOperator, UpContext> upContexts) -> UpResult`
 *   Drops the OperatorContext, for rules that do not need information from the top-down pass.
 * - `FunctionUpAlt3`: `(LogicalOperator op, std::vector<LogicalOperator> newChildren) -> UpResult`
 *   Drops both OperatorContext and UpContexts, for rules that only rebuild op from its
 *   already-rebuilt children.
 *
 * Each constructor takes a `std::variant` of the full callback and its alternatives, so any
 * of these forms can be passed directly; PlanVisitor normalizes them internally to the full
 * `FunctionDown`/`FunctionUp` signature.
 */

namespace NES
{
template <typename OperatorContext = std::monostate, typename DownContext = std::monostate, typename UpContext = std::monostate>
class PlanVisitor
{
public:
    struct DownResult
    {
        OperatorContext operatorContext;
        std::unordered_map<LogicalOperator, DownContext> downContexts;
    };

    struct UpResult
    {
        /// NOLINTNEXTLINE(google-explicit-constructor)
        UpResult(LogicalOperator op) : op(std::move(op)), upContext({}) { };
        UpResult(LogicalOperator op, UpContext upContext) : op(std::move(op)), upContext(std::move(upContext)) { };
        LogicalOperator op;
        UpContext upContext;
    };

    /// Main callbacks
    using FunctionDown = std::function<DownResult(LogicalOperator, std::vector<DownContext>)>;
    using FunctionUp = std::function<UpResult(
        LogicalOperator, std::vector<LogicalOperator>, OperatorContext, std::unordered_map<LogicalOperator, UpContext>)>;

    /// Alternative callbacks
    using FunctionDownAlt = std::function<DownResult(LogicalOperator)>;
    using FunctionUpAlt1 = std::function<UpResult(LogicalOperator, std::vector<LogicalOperator>, OperatorContext)>;
    using FunctionUpAlt2
        = std::function<UpResult(LogicalOperator, std::vector<LogicalOperator>, std::unordered_map<LogicalOperator, UpContext>)>;
    using FunctionUpAlt3 = std::function<UpResult(LogicalOperator, std::vector<LogicalOperator>)>;

    explicit PlanVisitor(
        std::variant<FunctionDown, FunctionDownAlt> fnDown, std::variant<FunctionUp, FunctionUpAlt1, FunctionUpAlt2, FunctionUpAlt3> fnUp)
        : fnDown(normalizeFnDown(fnDown)), fnUp(normalizeFnUp(fnUp)) { };

    explicit PlanVisitor(std::variant<FunctionDown, FunctionDownAlt> fnDown) : PlanVisitor(fnDown, noOpUp) { };
    explicit PlanVisitor(std::variant<FunctionUp, FunctionUpAlt1, FunctionUpAlt2, FunctionUpAlt3> fnUp) : PlanVisitor(noOpDown, fnUp) { };

    [[nodiscard]] LogicalPlan apply(LogicalPlan plan)
    {
        Instance instance{fnDown, fnUp};

        instance.init(plan);
        instance.traverseDown(plan);
        instance.traverseUp();
        const auto newRoots = instance.getNewRoots(plan);

        return plan.withRootOperators(newRoots);
    }

private:
    struct Instance
    {
        Instance(FunctionDown fnDown, FunctionUp fnUp) : fnDown(std::move(fnDown)), fnUp(std::move(fnUp)) { };

        void init(const LogicalPlan& plan)
        {
            std::stack<LogicalOperator> toVisit;
            std::unordered_set<LogicalOperator> visited;

            for (const auto& op : plan.getRootOperators())
            {
                toVisit.push(op);
            }

            while (!toVisit.empty())
            {
                auto op = toVisit.top();
                toVisit.pop();
                if (visited.contains(op))
                {
                    continue;
                }
                visited.insert(op);

                childCounter.emplace(op, op.getChildren().size());

                for (const auto& child : op.getChildren())
                {
                    parentCounter[child]++;
                    parentMap[child].emplace_back(op);
                    toVisit.push(child);
                }
            }
        }

        void traverseDown(const LogicalPlan& plan)
        {
            std::stack<LogicalOperator> toVisit;

            for (const auto& root : plan.getRootOperators() | std::views::reverse)
            {
                toVisit.push(root);
            }

            while (!toVisit.empty())
            {
                auto op = toVisit.top();
                toVisit.pop();

                auto [opCon, childCon] = fnDown(op, std::move(downContextMap[op]));

                operatorContextMap.insert_or_assign(op, opCon);

                for (auto child : op.getChildren())
                {
                    auto itr = childCon.find(child);
                    downContextMap[child].emplace_back(itr == childCon.end() ? DownContext{} : itr->second);
                }

                if (op.getChildren().empty())
                {
                    leaves.emplace_back(op);
                }

                for (const auto& child : op.getChildren() | std::views::reverse)
                {
                    parentCounter[child]--;
                    if (parentCounter[child] == 0)
                    {
                        toVisit.push(child);
                    }
                }
            }
        }

        void traverseUp()
        {
            std::stack<LogicalOperator> toVisit;

            for (const auto& leaf : leaves | std::views::reverse)
            {
                toVisit.push(leaf);
            }

            while (!toVisit.empty())
            {
                auto op = toVisit.top();
                toVisit.pop();

                auto context = operatorContextMap.at(op);
                std::vector<LogicalOperator> newChildren;

                std::unordered_map<LogicalOperator, UpContext> upContext;

                for (const auto& child : op.getChildren())
                {
                    auto newChild = newOperatorMap.at(child);
                    newChildren.emplace_back(newChild);
                    upContext.emplace(newChild, upContextMap.at(child));
                }

                auto [newOp, newUpContext] = fnUp(op, newChildren, context, upContext);

                INVARIANT(
                    !newOperatorMap.contains(op),
                    "each operator can only be inserted once into newOperatorMap since it is only visited once");
                newOperatorMap.insert_or_assign(op, std::move(newOp));
                INVARIANT(
                    !upContextMap.contains(op), "each operator can only be inserted once into upContextMap since it is only visited once");
                upContextMap.insert_or_assign(op, std::move(newUpContext));

                if (auto it = parentMap.find(op); it != parentMap.end())
                {
                    for (const auto& parent : it->second)
                    {
                        childCounter[parent]--;
                        if (childCounter[parent] == 0)
                        {
                            toVisit.push(parent);
                        }
                    }
                }
            }
        }

        std::vector<LogicalOperator> getNewRoots(const LogicalPlan& plan)
        {
            std::vector<LogicalOperator> newRoots;
            for (const auto& root : plan.getRootOperators())
            {
                newRoots.emplace_back(newOperatorMap.at(root));
            }

            return newRoots;
        }

        FunctionDown fnDown;
        FunctionUp fnUp;
        std::unordered_map<LogicalOperator, uint64_t> parentCounter;
        std::unordered_map<LogicalOperator, std::vector<LogicalOperator>> parentMap;
        std::unordered_map<LogicalOperator, uint64_t> childCounter;

        std::vector<LogicalOperator> leaves;
        std::unordered_map<LogicalOperator, std::vector<DownContext>> downContextMap{};
        std::unordered_map<LogicalOperator, UpContext> upContextMap{};
        std::unordered_map<LogicalOperator, OperatorContext> operatorContextMap{};

        std::unordered_map<LogicalOperator, LogicalOperator> newOperatorMap;
    };

    FunctionDown fnDown;
    FunctionUp fnUp;

    FunctionDown normalizeFnDown(std::variant<FunctionDown, FunctionDownAlt> function)
    {
        return std::visit(
            Overloaded{
                [](FunctionDown fnDown) -> FunctionDown { return fnDown; },
                [](FunctionDownAlt fnDown) -> FunctionDown
                { return [fnDown](LogicalOperator op, std::vector<DownContext>) -> DownResult { return fnDown(op); }; }},
            function);
    }

    FunctionUp normalizeFnUp(std::variant<FunctionUp, FunctionUpAlt1, FunctionUpAlt2, FunctionUpAlt3> function)
    {
        return std::visit(
            Overloaded{
                [](FunctionUp fnUp) -> FunctionUp { return fnUp; },
                [](FunctionUpAlt1 fnUp) -> FunctionUp
                {
                    return [fnUp](
                               LogicalOperator op,
                               std::vector<LogicalOperator> children,
                               OperatorContext operatorContext,
                               std::unordered_map<LogicalOperator, UpContext>) { return fnUp(op, std::move(children), operatorContext); };
                },
                [](FunctionUpAlt2 fnUp) -> FunctionUp
                {
                    return [fnUp](
                               LogicalOperator op,
                               std::vector<LogicalOperator> children,
                               OperatorContext,
                               std::unordered_map<LogicalOperator, UpContext> upContexts)
                    { return fnUp(op, std::move(children), upContexts); };
                },
                [](FunctionUpAlt3 fnUp) -> FunctionUp
                {
                    return [fnUp](
                               LogicalOperator op,
                               std::vector<LogicalOperator> children,
                               OperatorContext,
                               std::unordered_map<LogicalOperator, UpContext>) { return fnUp(op, std::move(children)); };
                },
            },
            function);
    }

    static DownResult noOpDown(const LogicalOperator&, const std::vector<DownContext>&) { return {}; }

    static UpResult noOpUp(
        const LogicalOperator& op,
        std::vector<LogicalOperator> children,
        const OperatorContext&,
        const std::unordered_map<LogicalOperator, UpContext>&)
    {
        return op.withChildren(std::move(children));
    }
};
}
