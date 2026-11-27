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

#include <Operators/Windows/JoinFunctionSplit.hpp>

#include <optional>
#include <ranges>
#include <vector>

#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
void collectConjuncts(const LogicalFunction& function, std::vector<LogicalFunction>& conjuncts)
{
    if (function.tryGetAs<AndLogicalFunction>().has_value())
    {
        for (const auto& child : function.getChildren())
        {
            collectConjuncts(child, conjuncts);
        }
        return;
    }
    conjuncts.push_back(function);
}

std::optional<EquiJoinKey> tryGetEquiJoinKey(const LogicalFunction& conjunct, const OperatorId leftId, const OperatorId rightId)
{
    if (not conjunct.tryGetAs<EqualsLogicalFunction>().has_value())
    {
        return std::nullopt;
    }
    const auto children = conjunct.getChildren();
    if (children.size() != 2)
    {
        return std::nullopt;
    }
    const auto first = children[0].tryGetAs<FieldAccessLogicalFunction>();
    const auto second = children[1].tryGetAs<FieldAccessLogicalFunction>();
    if (not first.has_value() or not second.has_value())
    {
        return std::nullopt;
    }

    const auto firstField = first.value()->getField();
    const auto secondField = second.value()->getField();
    const auto firstProducer = firstField.getProducedBy().getId();
    const auto secondProducer = secondField.getProducedBy().getId();

    std::optional<EquiJoinKey> key;
    if (firstProducer == leftId and secondProducer == rightId)
    {
        key = EquiJoinKey{.left = firstField, .right = secondField};
    }
    else if (firstProducer == rightId and secondProducer == leftId)
    {
        key = EquiJoinKey{.left = secondField, .right = firstField};
    }
    else
    {
        return std::nullopt;
    }

    /// The hash map compares keys bytewise, so both sides must be castable to a common type.
    if (not key->left.getDataType().join(key->right.getDataType()).has_value())
    {
        return std::nullopt;
    }
    return key;
}
}

SplitJoinFunction splitJoinFunction(const JoinLogicalOperator& join)
{
    const auto children = join.getBothChildren();
    const auto leftId = children[0].getId();
    const auto rightId = children[1].getId();

    std::vector<LogicalFunction> conjuncts;
    collectConjuncts(join.getJoinFunction(), conjuncts);

    SplitJoinFunction split;
    std::vector<LogicalFunction> residualConjuncts;
    for (const auto& conjunct : conjuncts)
    {
        if (auto key = tryGetEquiJoinKey(conjunct, leftId, rightId))
        {
            split.equiKeys.push_back(std::move(*key));
        }
        else
        {
            residualConjuncts.push_back(conjunct);
        }
    }

    if (residualConjuncts.empty())
    {
        return split;
    }
    if (residualConjuncts.size() == 1)
    {
        split.residual = residualConjuncts.front();
        return split;
    }

    /// Re-infer the data type of the rebuilt AND chain (boolean, nullable if any conjunct is), as the join operator did for its function.
    const std::vector<Field> inputFields = children | std::views::transform([](const auto& child) { return child.getOutputSchema(); })
        | std::views::join | std::ranges::to<std::vector>();
    const auto inputSchema = Schema<Field, Unordered>::tryCreateCollisionFree(inputFields);
    INVARIANT(inputSchema.has_value(), "The inputs of a bound join must not have colliding fields");

    LogicalFunction residual = residualConjuncts.front();
    for (const auto& conjunct : residualConjuncts | std::views::drop(1))
    {
        residual = AndLogicalFunction{residual, conjunct};
    }
    split.residual = residual.withInferredDataType(inputSchema.value());
    return split;
}

}
