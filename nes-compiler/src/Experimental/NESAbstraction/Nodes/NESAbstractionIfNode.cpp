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

#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionIfNode.hpp>
#include <utility>

NESAbstractionIfNode::NESAbstractionIfNode(uint64_t nodeId, bool isRootNode,
                                           std::vector<ComparisonType> comparisonType, 
                                           std::vector<int32_t> comparisonValues) :
                                           NESAbstractionNode(nodeId, isRootNode, NodeType::IfNode),
                                           comparisons(std::move(comparisonType)),
                                           comparisonValues(std::move(comparisonValues)){}

std::vector<NESAbstractionIfNode::ComparisonType> NESAbstractionIfNode::getComparisons(){
  return comparisons;
}

std::vector<int32_t> NESAbstractionIfNode::getComparisonValues() {
  return comparisonValues;
}

bool NESAbstractionIfNode::classof(const NESAbstractionNode *Node) {
  return Node->getNodeType() == NESAbstractionNode::IfNode;
}