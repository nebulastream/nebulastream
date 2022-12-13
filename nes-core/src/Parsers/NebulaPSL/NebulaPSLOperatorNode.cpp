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

#include <Parsers/NebulaPSL/NebulaPSLOperatorNode.hpp>

namespace NES::Parsers {
// creates a instance of the Operator Node (for as AST Tree node) with a unique identifier
NebulaPSLOperatorNode::NebulaPSLOperatorNode(int32_t id) { this->id = id; }
//Getter and Setter
int32_t NebulaPSLOperatorNode::getId() const { return this->id; }
void NebulaPSLOperatorNode::setId(int32_t id) { this->id = id; }
const std::string& NebulaPSLOperatorNode::getOperatorName() const { return this->operatorName; }
void NebulaPSLOperatorNode::setOperatorName(const std::string& operatorName) { this->operatorName = operatorName; }
int32_t NebulaPSLOperatorNode::getRightChildId() const { return this->rightChildId; }
void NebulaPSLOperatorNode::setRightChildId(int32_t rightChildId) { this->rightChildId = rightChildId; }
int32_t NebulaPSLOperatorNode::getLeftChildId() const { return this->leftChildId; }
void NebulaPSLOperatorNode::setLeftChildId(int32_t leftChildId) { this->leftChildId = leftChildId; }
const std::pair<int32_t, int32_t>& NebulaPSLOperatorNode::getMinMax() const { return this->minMax; }
void NebulaPSLOperatorNode::setMinMax(const std::pair<int32_t, int32_t>& minMax) { this->minMax = minMax; }
int32_t NebulaPSLOperatorNode::getParentNodeId() const { return this->parentNodeId; }
void NebulaPSLOperatorNode::setParentNodeId(int32_t parentNodeId) { this->parentNodeId = parentNodeId; }

}// namespace NES::Parsers