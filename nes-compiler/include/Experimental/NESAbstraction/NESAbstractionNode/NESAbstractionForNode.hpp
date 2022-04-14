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

#ifndef NES_INCLUDE_EXPERIMENTAL_NESABSTRACTIONFORNODE_HPP_
#define NES_INCLUDE_EXPERIMENTAL_NESABSTRACTIONFORNODE_HPP_

#ifdef MLIR_COMPILER

#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionNode.hpp>
#include <vector>

class NESAbstractionForNode : public NESAbstractionNode {
public:
    NESAbstractionForNode(uint64_t nodeId, bool isRootNode, uint64_t upperBound, std::vector<uint64_t> indexes, std::vector<NESAbstractionNode::BasicType> types);

    ~NESAbstractionForNode() override = default;

    uint64_t getUpperBound() const;
    std::vector<uint64_t> getIndexes() const;
    std::vector<NESAbstractionNode::BasicType> getTypes() const;

    static bool classof(const NESAbstractionNode *Node);

private:
    uint64_t upperBound;
    std::vector<uint64_t> indexes;
    std::vector<NESAbstractionNode::BasicType> types;
    std::vector<std::shared_ptr<NESAbstractionNode>> childNodes;
};
#endif //MLIR_COMPILER
#endif //NES_INCLUDE_EXPERIMENTAL_NESABSTRACTIONFORNODE_HPP_
