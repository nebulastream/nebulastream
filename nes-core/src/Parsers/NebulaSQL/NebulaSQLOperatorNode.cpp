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

#include <Parsers/NebulaSQL/NebulaSQLOperatorNode.hpp>

namespace NES::Parsers {

NebulaSQLOperatorNode::NebulaSQLOperatorNode(int32_t id) : id(id) {}

int32_t NebulaSQLOperatorNode::getId() const { return id; }
void NebulaSQLOperatorNode::setId(int32_t id) { this->id = id; }

const std::string& NebulaSQLOperatorNode::getOperatorName() const { return operatorName; }
void NebulaSQLOperatorNode::setOperatorName(const std::string& operatorName) { this->operatorName = operatorName; }

const std::pair<int32_t, int32_t>& NebulaSQLOperatorNode::getMinMax() const { return minMax; }
void NebulaSQLOperatorNode::setMinMax(const std::pair<int32_t, int32_t>& minMax) { this->minMax = minMax; }

int32_t NebulaSQLOperatorNode::getParentNodeId() const { return parentNodeId; }
void NebulaSQLOperatorNode::setParentNodeId(int32_t parentNodeId) { this->parentNodeId = parentNodeId; }

void NebulaSQLOperatorNode::addChild(std::shared_ptr<NebulaSQLOperatorNode> child) {
    children.push_back(child);
}

std::vector<std::shared_ptr<NebulaSQLOperatorNode>>& NebulaSQLOperatorNode::getChildren() {
    return children;
}
}// namespace NES::Parsers