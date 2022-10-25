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

#include <Optimizer/Phases/TopologyReconfigurationPhase.hpp>
#include <Util/PlacementStrategy.hpp>
#include <Topology/Topology.hpp>
namespace NES::Optimizer::Experimental {
//todo: at which point should the topology actually be changed? here of before
//todo: pass topologyNodes or ids here?
bool TopologyReconfigurationPhase::execute(PlacementStrategy::Value placementStrategy, const SharedQueryPlanPtr& sharedQueryPlan, uint64_t movingNode, uint64_t oldParent, uint64_t newParent) {
   auto commonAncestor = getCommonAncestor(oldParent, newParent);
   auto pathFromOldParent = topology->findNodesBetween(oldParent, commonAncestor);
   //iterate over all operators and check if they are places on the path from old parent, if they are not, pin them where they are


}
TopologyNodePtr TopologyReconfigurationPhase::getCommonAncestor(uint64_t oldParent, uint64_t newParent) {
    //todo: error handling
    /*
    auto oldParentNode = topology->findNodeWithId(oldParent);
    auto newParentNode = topology->findNodeWithId(newParent);
     */
    return topology->findCommonAncestor(oldParent, newParent);
}
}
