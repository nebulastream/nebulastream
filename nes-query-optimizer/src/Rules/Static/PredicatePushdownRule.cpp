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

#include <Rules/Static/PredicatePushdownRule.hpp>

#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>

namespace NES
{

LogicalPlan PredicatePushdownRule::apply(LogicalPlan queryPlan) const
{
    /// TODO Adapt for multiple roots
    // std::cerr << explain(queryPlan, ExplainVerbosity::Debug) << '\n';

    auto originalRoots = queryPlan.getRootOperators();
    PRECONDITION(originalRoots.size() == 1, "predicate pushdown not yet implemented for more than one root");

    auto newRoot = predicatePushdown(originalRoots.at(0), {});

    queryPlan = queryPlan.withRootOperators({newRoot});

    // std::cerr << explain(queryPlan, ExplainVerbosity::Debug) << '\n';

    return queryPlan;
}

LogicalOperator PredicatePushdownRule::predicatePushdown(LogicalOperator op, const std::vector<LogicalFunction>& predicateSet) const
{
    // std::cerr << op.getName() << '\n';

    if (auto sourceOp = op.tryGetAs<SourceDescriptorLogicalOperator>(); sourceOp)
    {
        return op;
    }
    if (auto selectionOp = op.tryGetAs<SelectionLogicalOperator>(); selectionOp)
    {
        return predicatePushdownSelection(selectionOp.value(), predicateSet);
    }
    if (auto unionOp = op.tryGetAs<UnionLogicalOperator>(); unionOp)
    {
        return predicatePushdownUnion(unionOp.value(), predicateSet);
    }

    if (predicateSet.empty())
    {
        std::vector<LogicalOperator> children;
        std::vector<Schema> schemas;
        for (const auto& child: op.getChildren())
        {
            auto newChild = predicatePushdown(child, predicateSet);
            schemas.push_back(newChild.getOutputSchema());
            children.push_back(newChild);
        }
        return op.withChildren(children).withInferredSchema(schemas);
    }

    return addSelection(op, predicateSet);
}

LogicalOperator PredicatePushdownRule::addSelection(LogicalOperator op, std::vector<LogicalFunction> predicateSet) const
{
    LogicalFunction newPredicate = predicateSet.back();
    predicateSet.pop_back();

    while (!predicateSet.empty())
    {
        newPredicate = AndLogicalFunction{newPredicate, predicateSet.back()};
        predicateSet.pop_back();
    }

    std::vector<LogicalOperator> children;
    std::vector<Schema> schemas;
    for (const auto &child: op.getChildren())
    {
        auto newChild = predicatePushdown(child, {});
        schemas.push_back(newChild.getOutputSchema());
        children.push_back(newChild);
    }

    return SelectionLogicalOperator{newPredicate}.withChildren(children).withInferredSchema(schemas);
}

LogicalOperator PredicatePushdownRule::predicatePushdownSelection(TypedLogicalOperator<SelectionLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const
{
    // Selection operators only have one child.
    predicateSet.emplace_back(op->getPredicate());
    return predicatePushdown(op.getChildren().at(0), predicateSet);
}

LogicalOperator PredicatePushdownRule::predicatePushdownUnion(TypedLogicalOperator<UnionLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const
{
    std::vector<LogicalOperator> children;
    std::vector<Schema> schemas;
    for (auto child: op.getChildren())
    {
        auto newChild = predicatePushdown(child, predicateSet);
        schemas.push_back(newChild.getOutputSchema());
        children.emplace_back(newChild);
    }
    return UnionLogicalOperator{}.withChildren(children).withInferredSchema(schemas);
}

const std::type_info& PredicatePushdownRule::getType()
{
    return typeid(PredicatePushdownRule);
}

std::string_view PredicatePushdownRule::getName()
{
    return NAME;
}

std::set<std::type_index> PredicatePushdownRule::dependsOn() const
{
    return {};
}

std::set<std::type_index> PredicatePushdownRule::requiredBy() const
{
    return {};
}

bool PredicatePushdownRule::operator==(const PredicatePushdownRule& /*other*/) const
{
    return true;
}
}