/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)
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

#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionNode.hpp>


NESAbstractionNode::NESAbstractionNode(uint64_t nodeId, bool isRootNode, NodeType nodeType) : nodeId(nodeId), isRootNode(isRootNode), nodeType(nodeType){}

void NESAbstractionNode::setChildNode(std::shared_ptr<NESAbstractionNode> newChildNode) {
  childNodes.push_back(std::move(newChildNode));
}

std::vector<std::shared_ptr<NESAbstractionNode>> NESAbstractionNode::getChildNodes() {
  return std::move(childNodes);
}

uint64_t NESAbstractionNode::getNodeId() const {
  return nodeId;
}

bool NESAbstractionNode::getIsRootNode() const {
  return isRootNode;
}

NESAbstractionNode::NodeType NESAbstractionNode::getNodeType() const {
  return nodeType;
}


