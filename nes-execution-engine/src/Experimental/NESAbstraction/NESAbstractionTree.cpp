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

//#import <NESAbstraction/NESAbstractionTree.hpp>
#include <Experimental/NESAbstraction/NESAbstractionTree.hpp>
#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionNode.hpp>

NESAbstractionTree::NESAbstractionTree(std::shared_ptr<NESAbstractionNode> rootNode, uint64_t queryPlanId, uint64_t depth,
                                       uint64_t numNodes) : rootNode(std::move(rootNode)), queryPlanId(queryPlanId),
                                       depth(depth), numNodes(numNodes){}

std::shared_ptr<NESAbstractionNode> NESAbstractionTree::getRootNode() {
  return std::move(rootNode);
}

uint64_t NESAbstractionTree::getQueryPlanId() const {
  return queryPlanId;
}

uint64_t NESAbstractionTree::getDepth() const {
  return depth;
}

uint64_t NESAbstractionTree::getNumNodes() const {
  return numNodes;
}
