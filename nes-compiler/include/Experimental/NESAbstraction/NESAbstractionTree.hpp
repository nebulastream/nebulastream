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

#ifndef NES_INCLUDE_EXPERIMENTAL_NESABSTRACTIONTREE_HPP_
#define NES_INCLUDE_EXPERIMENTAL_NESABSTRACTIONTREE_HPP_

#ifdef MLIR_COMPILER

#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionNode.hpp>
#include <cstdint>
#include <memory>

class NESAbstractionTree {
public:
    NESAbstractionTree(std::shared_ptr<NESAbstractionNode> rootNode, uint64_t queryPlanId, uint64_t depth, uint64_t numNodes);
    ~NESAbstractionTree() = default;

    std::shared_ptr<NESAbstractionNode> getRootNode();

    uint64_t getQueryPlanId() const;

    uint64_t getDepth() const;

    uint64_t getNumNodes() const;


private:
    std::shared_ptr<NESAbstractionNode> rootNode;
    uint64_t queryPlanId;
    uint64_t depth;
    uint64_t numNodes;
};
#endif //MLIR_COMPILER
#endif //NES_INCLUDE_EXPERIMENTAL_NESABSTRACTIONTREE_HPP_
