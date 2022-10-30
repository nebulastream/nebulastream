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
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Topology/TopologyNode.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Topology/Topology.hpp>

namespace NES::Optimizer::Experimental {

const std::string PINNED_NODE_ID = "PINNED_NODE_ID";
const std::string PLACED = "PLACED";
//todo: at which point should the topology actually be changed? here of before
//todo: pass topologyNodes or ids here?
TopologyReconfigurationPhase::TopologyReconfigurationPhase(GlobalExecutionPlanPtr globalExecutionPlan,
TopologyPtr topology) : globalExecutionPlan(globalExecutionPlan), topology(topology) {};

bool TopologyReconfigurationPhase::execute(PlacementStrategy::Value placementStrategy, const SharedQueryPlanPtr& sharedQueryPlan, uint64_t movingNode, uint64_t oldParent, uint64_t newParent) {
    (void) movingNode;
   auto commonAncestor = getCommonAncestor(oldParent, newParent);
   //todo: restructure so we only need to call find node with id once
    auto oldParentNode = topology->findNodeWithId(oldParent);
   auto pathFromOldParent = topology->findNodesBetween(oldParentNode, commonAncestor);
   //todo; construct a set from the nodes here? could speed up execution further down
   //iterate over all operators and check if they are places on the path from old parent, if they are not, pin them where they are
   //or take the oposite approach and only put these down as not placed
   //todo: what does the "snapshot" do here and should we use it?

   //first do an optimistic approach, assuming we have enough resources on our path
   //when that does not work, we run this again but unplace more operators
    auto operators = QueryPlanIterator(sharedQueryPlan->getQueryPlan()).snapshot();
    for (auto op : operators) {
        //todo: execution node has the same id as topology node and it has a map of the query subplans running on it
        //todo: we need a mapping of placed operator nodes to the execution nodes they are placed on
        //todo: but this mapping exists only inside the placement strategy
        //todo: idea: put the node number into the placed attirbute instead of just a bool
        //todo: or is there another way to keep that infromation, after the operators are placed?


        //the above is probably all wrong. pinned_node_id is for all nodes that were somehow placed, see bottomUp


        auto pinnedNodeId = std::any_cast<uint64_t>(op->as<OperatorNode>()->getProperty(PINNED_NODE_ID));
        //todo: can we also get these as execution node?
        //how can we get the node which placed operators where places on and not just a bool indicating that they have been placed
        //auto placedNodeId = op->as<OperatorNode>()->getProperty("PINNED_NODE_ID");
        //can we do this better than with 2 nested loops
        //look at how topologyMap is constructed in BasePlacementStretegy
        for (auto topologyNode : pathFromOldParent) {
            if (topologyNode->getId() == pinnedNodeId) {
                op->as<OperatorNode>()->removeProperty(PINNED_NODE_ID);
                op->as<OperatorNode>()->removeProperty(PLACED);

                //todo: there is a getTopologyNode() function in execution node, but can i also get it the other way araound?
            }
        }
    }
    queryPlacementPhase->execute(placementStrategy, sharedQueryPlan);
    //todo: when to return false?
    return true;
}
TopologyNodePtr TopologyReconfigurationPhase::getCommonAncestor(uint64_t oldParent, uint64_t newParent) {
    //todo: error handling
    auto oldParentNode = topology->findNodeWithId(oldParent);
    auto newParentNode = topology->findNodeWithId(newParent);
    return topology->findCommonAncestor({oldParentNode, newParentNode});
}
}
