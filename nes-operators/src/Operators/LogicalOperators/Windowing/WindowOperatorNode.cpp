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

#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>

namespace NES {

WindowOperatorNode::WindowOperatorNode(Windowing::LogicalWindowDefinitionPtr const& windowDefinition,
                                       OperatorId id,
                                       OriginId originId)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), OriginIdAssignmentOperator(id, originId),
      windowDefinition(windowDefinition) {}

Windowing::LogicalWindowDefinitionPtr WindowOperatorNode::getWindowDefinition() const { return windowDefinition; }

const std::vector<OriginId> WindowOperatorNode::getOutputOriginIds() const {
    return OriginIdAssignmentOperator::getOutputOriginIds();
}
void WindowOperatorNode::setOriginId(OriginId originId) {
    OriginIdAssignmentOperator::setOriginId(originId);
    windowDefinition->setOriginId(originId);
}

}// namespace NES