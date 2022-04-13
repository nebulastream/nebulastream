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

#ifndef MLIR_APPROACH_NESABSTRACTIONWRITENODE_HPP
#define MLIR_APPROACH_NESABSTRACTIONWRITENODE_HPP

#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionNode.hpp>
#include <vector>

class NESAbstractionWriteNode : public NESAbstractionNode {
public:
    NESAbstractionWriteNode(uint64_t nodeId, bool isRootNode, std::vector<NESAbstractionNode::BasicType> outputTypes);
    ~NESAbstractionWriteNode() override = default;

    std::vector<NESAbstractionNode::BasicType> getOutputTypes();

    static bool classof(const NESAbstractionNode *Node);

private:
    std::vector<NESAbstractionNode::BasicType> outputTypes;
    std::vector<std::shared_ptr<NESAbstractionNode>> childNodes;
};
#endif //MLIR_APPROACH_NESABSTRACTIONWRITENODE_HPP
