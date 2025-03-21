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
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/NodeFunctionBinary.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{
std::pair<IdentifierList, IdentifierList> findEquiJoinKeyNames(std::shared_ptr<NES::NodeFunction> joinFunction)
{
    IdentifierList leftJoinKeyNameEqui{};
    IdentifierList rightJoinKeyNameEqui{};

    /// Maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<std::shared_ptr<NES::NodeFunctionBinary>> visitedFunctions;

    NES_DEBUG("Iterate over all NodeFunction to check join field.");

    auto bfsIterator = BreadthFirstNodeIterator(joinFunction);
    for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr)
    {
        if (Util::instanceOf<NodeFunctionBinary>(*itr))
        {
            auto visitingOp = NES::Util::as<NodeFunctionBinary>(*itr);
            if (visitedFunctions.contains(visitingOp))
            {
                /// skip rest of the steps as the node found in already visited node list
                continue;
            }
            else
            {
                visitedFunctions.insert(visitingOp);
                ///Find the schema for left and right join key
                if (!Util::instanceOf<NodeFunctionBinary>(Util::as<NodeFunctionBinary>((*itr))->getLeft())
                    && Util::instanceOf<NodeFunctionEquals>((*itr)))
                {
                    const auto leftJoinKey = Util::as<NodeFunctionFieldAccess>(Util::as<NodeFunctionBinary>((*itr))->getLeft());
                    leftJoinKeyNameEqui = leftJoinKey->getFieldName();

                    const auto rightJoinKey = Util::as<NodeFunctionFieldAccess>(Util::as<NodeFunctionBinary>((*itr))->getRight());
                    rightJoinKeyNameEqui = rightJoinKey->getFieldName();

                    NES_DEBUG("LogicalJoinOperator: Inserting operator in collection of already visited node.");
                    visitedFunctions.insert(visitingOp);
                } /// if Equals
            } /// else new node
        } /// if binary function
    } /// for
    return std::make_pair(leftJoinKeyNameEqui, rightJoinKeyNameEqui);
}
}
