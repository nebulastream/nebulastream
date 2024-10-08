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

#include <string>
#include <unordered_set>
#include <Expressions/BinaryExpressionNode.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{
std::pair<std::basic_string<char>, std::basic_string<char>> findEquiJoinKeyNames(std::shared_ptr<NES::ExpressionNode> joinExpression)
{
    std::basic_string<char> leftJoinKeyNameEqui;
    std::basic_string<char> rightJoinKeyNameEqui;

    /// Maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<std::shared_ptr<NES::BinaryExpressionNode>> visitedExpressions;

    NES_DEBUG("Iterate over all ExpressionNode to check join field.");

    auto bfsIterator = BreadthFirstNodeIterator(joinExpression);
    for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr)
    {
        if (Util::instanceOf<BinaryExpressionNode>(*itr))
        {
            auto visitingOp = NES::Util::as<BinaryExpressionNode>(*itr);
            if (visitedExpressions.contains(visitingOp))
            {
                /// skip rest of the steps as the node found in already visited node list
                continue;
            }
            else
            {
                visitedExpressions.insert(visitingOp);
                ///Find the schema for left and right join key
                if (!Util::instanceOf<BinaryExpressionNode>(Util::as<BinaryExpressionNode>(*itr)->getLeft())
                    && Util::instanceOf<EqualsExpressionNode>(*itr))
                {
                    const auto leftJoinKey = Util::as<FieldAccessExpressionNode>(Util::as<BinaryExpressionNode>(*itr)->getLeft());
                    leftJoinKeyNameEqui = leftJoinKey->getFieldName();

                    const auto rightJoinKey = Util::as<FieldAccessExpressionNode>(Util::as<BinaryExpressionNode>(*itr)->getRight());
                    rightJoinKeyNameEqui = rightJoinKey->getFieldName();

                    NES_DEBUG("LogicalJoinOperator: Inserting operator in collection of already visited node.");
                    visitedExpressions.insert(visitingOp);
                } /// if Equals
            } /// else new node
        } /// if binary expression
    } /// for
    return std::make_pair(leftJoinKeyNameEqui, rightJoinKeyNameEqui);
}
} /// namespace NES
