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

#include "Components/NesCoordinator.hpp"
#include "GRPC/WorkerRPCClient.hpp"
#include "Plans/Global/Query/GlobalQueryPlan.hpp"
#include "Runtime/RuntimeForwardRefs.hpp"
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/FaultToleranceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <log4cxx/helpers/exception.h>
#include <utility>

namespace NES::Optimizer {

std::unique_ptr<BasePlacementStrategy> BottomUpStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                                TopologyPtr topology,
                                                                TypeInferencePhasePtr typeInferencePhase) {
    return std::make_unique<BottomUpStrategy>(
        BottomUpStrategy(std::move(globalExecutionPlan), std::move(topology), std::move(typeInferencePhase)));
}

BottomUpStrategy::BottomUpStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                   TopologyPtr topology,
                                   TypeInferencePhasePtr typeInferencePhase)
    : BasePlacementStrategy(std::move(globalExecutionPlan), std::move(topology), std::move(typeInferencePhase)) {}

bool BottomUpStrategy::updateGlobalExecutionPlan(QueryId queryId,
                                                 FaultToleranceType faultToleranceType,
                                                 LineageType lineageType,
                                                 const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                                 const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {

    try {
        NES_DEBUG("Perform placement of the pinned and all their downstream operators.");
        // 1. Find the path where operators need to be placed
        performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 2. Place operators on the selected path
        performOperatorPlacement(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        //3. Check fault tolerance
        //FaultToleranceType ftApproach = checkFaultTolerance(globalExecutionPlan, topology, queryId);
        //faultToleranceType = checkFaultTolerance(globalExecutionPlan, topology, queryId);
        //NES_INFO("\nCHOSEN FAULT-TOLERANCE APPROACH: " + toString(faultToleranceType));

        // 4. add network source and sink operators
        addNetworkSourceAndSinkOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);



        // 5. Perform type inference on all updated query plans
        return runTypeInferencePhase(queryId, faultToleranceType, lineageType);
    } catch (log4cxx::helpers::Exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
}

void BottomUpStrategy::performOperatorPlacement(QueryId queryId,
                                                const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                                const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {

    NES_DEBUG("BottomUpStrategy: Get the all source operators for performing the placement.");
    for (auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        NES_DEBUG("BottomUpStrategy: Get the topology node for source operator " << pinnedUpStreamOperator->toString()
                                                                                 << " placement.");

        NES_INFO("\n Topology before" + topology->toString());

        auto nodeId = std::any_cast<uint64_t>(pinnedUpStreamOperator->getProperty(PINNED_NODE_ID));
        TopologyNodePtr candidateTopologyNode = getTopologyNode(nodeId);

        // 1. If pinned up stream node was already placed then place all its downstream operators
        if (pinnedUpStreamOperator->hasProperty(PLACED) && std::any_cast<bool>(pinnedUpStreamOperator->getProperty(PLACED))) {
            //Fetch the execution node storing the operator
            operatorToExecutionNodeMap[pinnedUpStreamOperator->getId()] = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
            //Place all downstream nodes
            for (auto& downStreamNode : pinnedUpStreamOperator->getParents()) {
                placeOperator(queryId, downStreamNode->as<OperatorNode>(), candidateTopologyNode, pinnedDownStreamOperators);
                NES_INFO("\nTopology after " + topology->toString())
                GlobalQueryPlanPtr globalQueryPlan;

            }
        } else {// 2. If pinned operator is not placed then start by placing the operator
            if (candidateTopologyNode->getAvailableResources() == 0
                && !operatorToExecutionNodeMap.contains(pinnedUpStreamOperator->getId())) {
                NES_ERROR("BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
                throw log4cxx::helpers::Exception(
                    "BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
            }
            placeOperator(queryId, pinnedUpStreamOperator, candidateTopologyNode, pinnedDownStreamOperators);
            NES_INFO("\nTopology after " + topology->toString())
        }
    }
    NES_DEBUG("BottomUpStrategy: Finished placing query operators into the global execution plan");
}

void BottomUpStrategy::placeOperator(QueryId queryId,
                                     const OperatorNodePtr& operatorNode,
                                     TopologyNodePtr candidateTopologyNode,
                                     const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {

    if (operatorNode->hasProperty(PLACED) && std::any_cast<bool>(operatorNode->getProperty(PLACED))) {
        NES_DEBUG("Operator is already placed and thus skipping placement of this and its down stream operators.");
        return;
    }

    if (!operatorToExecutionNodeMap.contains(operatorNode->getId())) {

        NES_DEBUG("BottomUpStrategy: Place " << operatorNode);
        if ((operatorNode->hasMultipleChildrenOrParents() && !operatorNode->instanceOf<SourceLogicalOperatorNode>())
            || operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
            NES_TRACE("BottomUpStrategy: Received an NAry operator for placement.");
            //Check if all children operators already placed
            NES_TRACE("BottomUpStrategy: Get the topology nodes where child operators are placed.");
            std::vector<TopologyNodePtr> childTopologyNodes = getTopologyNodesForChildrenOperators(operatorNode);
            if (childTopologyNodes.empty()) {
                NES_WARNING(
                    "BottomUpStrategy: No topology node isOperatorAPinnedDownStreamOperator where child operators are placed.");
                return;
            }

            NES_TRACE("BottomUpStrategy: Find a node reachable from all topology nodes where child operators are placed.");
            if (childTopologyNodes.size() == 1) {
                candidateTopologyNode = childTopologyNodes[0];
            } else {
                candidateTopologyNode = topology->findCommonAncestor(childTopologyNodes);
            }
            if (!candidateTopologyNode) {
                NES_ERROR(
                    "BottomUpStrategy: Unable to find a common ancestor topology node to place the binary operator, operatorId: "
                    << operatorNode->getId());
                topology->print();
                throw log4cxx::helpers::Exception(
                    "BottomUpStrategy: Unable to find a common ancestor topology node to place the binary operator");
            }

            if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
                NES_TRACE("BottomUpStrategy: Received Sink operator for placement.");
                auto nodeId = std::any_cast<uint64_t>(operatorNode->getProperty(PINNED_NODE_ID));
                auto pinnedSinkOperatorLocation = getTopologyNode(nodeId);
                if (pinnedSinkOperatorLocation->getId() == candidateTopologyNode->getId()
                    || pinnedSinkOperatorLocation->containAsChild(candidateTopologyNode)) {
                    candidateTopologyNode = pinnedSinkOperatorLocation;
                } else {
                    NES_ERROR("BottomUpStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be "
                              "placed.");
                    throw log4cxx::helpers::Exception(

                        "BottomUpStrategy: Unexpected behavior. Could not find Topology node where sink operator is to be "
                        "placed.");
                }

                if (candidateTopologyNode->getAvailableResources() == 0) {
                    NES_ERROR("BottomUpStrategy: Topology node where sink operator is to be placed has no capacity.");
                    throw log4cxx::helpers::Exception(
                        "BottomUpStrategy: Topology node where sink operator is to be placed has no capacity.");
                }
            }
        }

        if (candidateTopologyNode->getAvailableResources() == 0) {

            NES_DEBUG("BottomUpStrategy: Find the next NES node in the path where operator can be placed");
            while (!candidateTopologyNode->getParents().empty()) {
                //FIXME: we are considering only one root node currently
                candidateTopologyNode = candidateTopologyNode->getParents()[0]->as<TopologyNode>();
                if (candidateTopologyNode->getAvailableResources() > 0) {
                    NES_DEBUG("BottomUpStrategy: Found NES node for placing the operators with id : "
                              << candidateTopologyNode->getId());
                    break;
                }
            }
        }

        if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("BottomUpStrategy: No node available for further placement of operators");
            throw log4cxx::helpers::Exception("BottomUpStrategy: No node available for further placement of operators");
        }

        NES_TRACE("BottomUpStrategy: Get the candidate execution node for the candidate topology node.");
        ExecutionNodePtr candidateExecutionNode = getExecutionNode(candidateTopologyNode);

        NES_TRACE("BottomUpStrategy: Get the candidate query plan where operator is to be appended.");
        QueryPlanPtr candidateQueryPlan = getCandidateQueryPlanForOperator(queryId, operatorNode, candidateExecutionNode);
        operatorNode->addProperty(PINNED_NODE_ID, candidateTopologyNode->getId());
        operatorNode->addProperty(PLACED, true);
        auto operatorCopy = operatorNode->copy();
        if (candidateQueryPlan->getRootOperators().empty()) {
            candidateQueryPlan->appendOperatorAsNewRoot(operatorCopy);
        } else {
            auto children = operatorNode->getChildren();
            for (const auto& child : children) {
                auto rootOperators = candidateQueryPlan->getRootOperators();
                if (candidateQueryPlan->hasOperatorWithId(child->as<OperatorNode>()->getId())) {
                    candidateQueryPlan->getOperatorWithId(child->as<OperatorNode>()->getId())->addParent(operatorCopy);
                }
                auto found =
                    std::find_if(rootOperators.begin(), rootOperators.end(), [child](const OperatorNodePtr& rootOperator) {
                        return rootOperator->getId() == child->as<OperatorNode>()->getId();
                    });
                if (found != rootOperators.end()) {
                    candidateQueryPlan->removeAsRootOperator(*(found));
                    auto updatedRootOperators = candidateQueryPlan->getRootOperators();
                    auto operatorAlreadyExistsAsRoot =
                        std::find_if(updatedRootOperators.begin(),
                                     updatedRootOperators.end(),
                                     [operatorCopy](const OperatorNodePtr& rootOperator) {
                                         return rootOperator->getId() == operatorCopy->as<OperatorNode>()->getId();
                                     });
                    if (operatorAlreadyExistsAsRoot == updatedRootOperators.end()) {
                        candidateQueryPlan->addRootOperator(operatorCopy);
                    }
                }
            }
            if (!candidateQueryPlan->hasOperatorWithId(operatorCopy->getId())) {
                candidateQueryPlan->addRootOperator(operatorCopy);
            }
        }

        NES_TRACE("BottomUpStrategy: Add the query plan to the candidate execution node.");
        if (!candidateExecutionNode->addNewQuerySubPlan(queryId, candidateQueryPlan)) {
            NES_ERROR("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query.");
            throw log4cxx::helpers::Exception("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query.");
        }
        NES_TRACE("BottomUpStrategy: Update the global execution plan with candidate execution node");
        globalExecutionPlan->addExecutionNode(candidateExecutionNode);

        NES_TRACE("BottomUpStrategy: Place the information about the candidate execution plan and operator id in the map.");
        operatorToExecutionNodeMap[operatorNode->getId()] = candidateExecutionNode;
        NES_DEBUG("BottomUpStrategy: Reducing the node remaining CPU capacity by 1");
        // Reduce the processing capacity by 1
        // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
        candidateTopologyNode->reduceResources(1);
        topology->reduceResources(candidateTopologyNode->getId(), 1);
    } else {
        candidateTopologyNode = operatorToExecutionNodeMap[operatorNode->getId()]->getTopologyNode();
    }

    auto isOperatorAPinnedDownStreamOperator = std::find_if(pinnedDownStreamOperators.begin(),
                                                            pinnedDownStreamOperators.end(),
                                                            [operatorNode](const OperatorNodePtr& pinnedDownStreamOperator) {
                                                                return pinnedDownStreamOperator->getId() == operatorNode->getId();
                                                            });

    if (isOperatorAPinnedDownStreamOperator != pinnedDownStreamOperators.end()) {
        NES_DEBUG("BottomUpStrategy: Found pinned downstream operator. Skipping placement of further operators.");
        return;
    }

    NES_TRACE("BottomUpStrategy: Place further upstream operators.");
    for (const auto& parent : operatorNode->getParents()) {
        placeOperator(queryId, parent->as<OperatorNode>(), candidateTopologyNode, pinnedDownStreamOperators);
    }
}
/*
FaultToleranceType BottomUpStrategy::checkFaultTolerance(GlobalExecutionPlanPtr globalExecutionPlan,
                                              TopologyPtr topology,
                                              QueryId queryId) {





    std::tuple<int,int> tup = {5,8};
    NES_INFO("TUP1: " + std::to_string(std::get<0>(tup)));


    std::vector<TopologyNodePtr> topologyNodes = std::vector<TopologyNodePtr>();
    std::vector<long> topologyIds;

    //Get IDs of all nodes in the topology

    topologyIds.push_back(topology->getRoot()->getId());
    topologyNodes.push_back(topology->getRoot());

    for(auto& child : topology->getRoot()->getAndFlattenAllChildren(false)){
        TopologyNodePtr nodeAsTopologyNode = child->as<TopologyNode>();
        topologyIds.push_back(nodeAsTopologyNode->getId());
        topologyNodes.push_back(child->as<TopologyNode>());
    }


    calcEffectiveValues(topologyNodes,queryId);

    for(auto& node : topologyNodes){
        node->setEffectiveLatency(node->getLatency());
        node->addNodeProperty("latency",std::tuple<float,float>{node->getLatency(),node->getLatency()});
    }

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    double totalQueryCost = 0;
    double operatorCount = 0;
    double avgOperatorCost = 0;

    double nodesCount = 0;
    double totalAvailableResources = 0;
    double totalUsedResources = 0;
    double totalResourceCapacity = 0;
    */

    /*if(!otherNodesAvailable(globalExecutionPlan, topologyIds,queryId)){
        NES_WARNING("\nFAULT-TOLERANCE CANNOT BE PROVIDED. THERE ARE NO POTENTIAL BACKUP NODES AVAILABLE.");
        return FaultToleranceType::NONE;
    }*/



/*
    double highestCost = 0;

    //Determine resource values of nodes from the Global Execution Plan after placing the query.
    for (auto& node : executionNodes) {

        if(node->getOccupiedResources(queryId) > highestCost){
            highestCost = node->getOccupiedResources(queryId);
        }

        double localNodeId = node->getId();
        nodesCount += 1;

        double localAvailableResources = topology->findNodeWithId(node->getId())->getAvailableResources();
        totalAvailableResources += localAvailableResources;

        double localUsedResources = node->getOccupiedResources(queryId);
        totalUsedResources += localUsedResources;

        double localResourceCapacity = localAvailableResources + localUsedResources;
        totalResourceCapacity += localResourceCapacity;

        NES_INFO("\n NodeID: " + std::to_string(localNodeId));
        NES_INFO("\nRESOURCE CAPACITY: "
                 + std::to_string(topology->findNodeWithId(node->getId())->getAvailableResources()
                                  + node->getOccupiedResources(queryId)))
        NES_INFO("\nAVAILABLE RESOURCES: " + std::to_string(topology->findNodeWithId(node->getId())->getAvailableResources()));
        NES_INFO("\nUSED RESOURCES: " + std::to_string(node->getOccupiedResources(queryId)));
    }

    double globalAvailableResources = 0;
    double globalUsedResources = 0;
    double globalResourceCapacity = 0;

    //Determine resource values of all nodes in the topology.
    for (auto& node : globalExecutionPlan->getAllExecutionNodes()) {
        double localAvailableResources = topology->findNodeWithId(node->getId())->getAvailableResources();
        globalAvailableResources += localAvailableResources;

        double localUsedResources = node->getOccupiedResources(queryId);
        globalUsedResources += localUsedResources;

        double localResourceCapacity = localAvailableResources + localUsedResources;
        globalResourceCapacity += localResourceCapacity;

        NES_INFO("\n NodeID: " + std::to_string(node->getId()));
        NES_INFO("\nRESOURCE CAPACITY: "
                 + std::to_string(topology->findNodeWithId(node->getId())->getAvailableResources()
                                  + node->getOccupiedResources(queryId)))
        NES_INFO("\nAVAILABLE RESOURCES: " + std::to_string(topology->findNodeWithId(node->getId())->getAvailableResources()));
        NES_INFO("\nUSED RESOURCES: " + std::to_string(node->getOccupiedResources(queryId)));
    }

    double avgCostPerNode = totalUsedResources / nodesCount;


    NES_INFO("\n==========QUERY SUMMARY========")
    NES_INFO("\nNumber of Nodes available: " + std::to_string(topologyIds.size() + 1) + " | Global Resource Capacity: "
             + std::to_string(globalResourceCapacity) + " | Globally available resource after deployment of query#"
             + std::to_string(queryId) + ": " + std::to_string(globalResourceCapacity - totalUsedResources));
    NES_INFO("\nNumber of Nodes used for query#" + std::to_string(queryId) + ": " + std::to_string(nodesCount)
             + " | Average resource cost per node: " + std::to_string(totalUsedResources / nodesCount));

    std::vector<ExecutionNodePtr> allNodes = globalExecutionPlan->getAllExecutionNodes();
    std::vector<int> activeStandbyCandidateIds;
    std::vector<int> checkpointingCandidateIds;
    bool activeStandbyPossible = false;
    bool checkpointingPossible = false;
*/

    //Check if there are nodes in the topology that can support the maximum query fragment cost. If there are such nodes, choose active standby.
    //TODO Potentially change the if condition so that only nodes are considered on which there is no fragment of the same query already running.
    //TODO Only consider nodes of the same depth
   /* for (auto& topologyId : topologyIds) {



        if (topology->findNodeWithId(topologyId)->getAvailableResources() < highestCost
            //&& (!globalExecutionPlan->checkIfExecutionNodeExists(topologyNode->getId()))
            ) {
            NES_WARNING("\n[CPU] ACTIVE_STANDBY not possible on node#" + std::to_string(topologyId) + " because it only has "
                        + std::to_string(topology->findNodeWithId(topologyId)->getAvailableResources()) + " available resources when "
                        + std::to_string(highestCost) + " was needed.")

        } else {
            activeStandbyCandidateIds.push_back(topologyId);
            NES_INFO("\n[CPU] ACTIVE_STANDBY: FOUND NODE#" + std::to_string(topologyId) + " WITH "
                     + std::to_string(topology->findNodeWithId(topologyId)->getAvailableResources()) + " AVAILABLE RESOURCES. "
                     + std::to_string(highestCost) + " NEEDED.")
            activeStandbyPossible = true;
        }

        if (topology->findNodeWithId(topologyId)->getResourceCapacity() < highestCost
            //&& (!globalExecutionPlan->checkIfExecutionNodeExists(topologyNode->getId()))
            ) {
            NES_WARNING("\n[CPU] CHECKPOINTING not possible on node#" + std::to_string(topologyId) + " because it only has a capacity of "
                        + std::to_string(topology->findNodeWithId(topologyId)->getAvailableResources()) + " when "
                        + std::to_string(highestCost) + " was needed.")

        } else {
            checkpointingCandidateIds.push_back(topologyId);
            NES_INFO("\n[CPU] CHECKPOINTING: FOUND NODE#" + std::to_string(topologyId) + " WITH "
                     + std::to_string(topology->findNodeWithId(topologyId)->getResourceCapacity()) + " CAPACITY. "
                     + std::to_string(highestCost) + " NEEDED.")
            checkpointingPossible = true;
        }
    }



    NES_INFO(topology->toString())
    //===========================



    for(auto& topNode : topologyNodes){
        NES_INFO("\nLatency on #" + std::to_string(topology->findNodeWithId(topNode->getId())->getId()) + " : " +
                 std::to_string(topology->findNodeWithId(topNode->getId())->getLatency()));
    }

    for(auto& enode : executionNodes){

        TopologyNodePtr node = topology->findNodeWithId(enode->getId());

        NES_INFO("\nEffective Ressources on #" + std::to_string(topology->findNodeWithId(enode->getId())->getId()) + " : " +
                     std::to_string(topology->findNodeWithId(enode->getId())->getEffectiveRessources()));

        NES_INFO("\nEffective Latency on #" + std::to_string(topology->findNodeWithId(enode->getId())->getId()) + " : " +
                 std::to_string(topology->findNodeWithId(enode->getId())->getEffectiveLatency()));

        NES_INFO("\nAvailable buffers: " + std::to_string(node->getAvailableBuffers()));
    }

    std::map<ExecutionNodePtr, int> map;
    std::vector<TopologyNodePtr> start;
    start.push_back(topology->getRoot());
    std::vector<TopologyNodePtr> end;
    end.push_back(topology->findNodeWithId(2));

    float highestEffectiveCost = 0;
    ExecutionNodePtr highestEffectiveCostNode;
    float highestEffectiveLatency = 0;
    ExecutionNodePtr highestEffectiveLatencyNode;

    //Find ExecutionNode with the highest effective cost and latency
    for (auto& exNode : executionNodes){

        if(topology->findNodeWithId(exNode->getId())->getEffectiveRessources() > highestEffectiveCost){
            highestEffectiveCost = topology->findNodeWithId(exNode->getId())->getEffectiveRessources();
            highestEffectiveCostNode = exNode;
        }
        if(topology->findNodeWithId(exNode->getId())->getEffectiveLatency() > highestEffectiveLatency){
            highestEffectiveLatency = topology->findNodeWithId(exNode->getId())->getEffectiveLatency();
            highestEffectiveLatencyNode = exNode;
        }
    }

    NES_INFO("HIGHEST EFFECTIVE COST NODE: " + highestEffectiveCostNode->toString())

    float highestExecutionPlanCost = calcExecutionPlanCosts(globalExecutionPlan);

    auto workerRpcClient = std::make_shared<WorkerRPCClient>();*/



    //TODO only consider TopologyNodes that are at the same depth as the highest effective cost node from the root.
    //Try out available TopologyNodes as backup nodes for the highest effective cost node and see what the highest possible cost would be.
    /*for(auto& topNode : topologyNodes){

        if(!globalExecutionPlan->checkIfExecutionNodeExists(topNode->getId())){
            std::tuple<int,float> tuple = {highestEffectiveCostNode->getOccupiedResources(queryId),
                                            topology->findNodeWithId(highestEffectiveCostNode->getId())->getEffectiveRessources()};
            std::map<int,float> map;
            map.insert({highestEffectiveCostNode->
                        getOccupiedResources(queryId),topology->
                        findNodeWithId(highestEffectiveCostNode->getId())->getEffectiveRessources()});

            //TODO find a way to obtain the OperatorNode of highestEffectiveCostNode and use it to create the backup node.
            OperatorNodePtr x;
            ExecutionNodePtr nodeEx = ExecutionNode::createExecutionNode(topNode,queryId,x);
            topology->findNodeWithId(nodeEx->getId())->addNodeProperty("ressources",tuple);
            for(auto& subPlan : highestEffectiveCostNode->getQuerySubPlans(queryId)){
                nodeEx->addNewQuerySubPlan(queryId,subPlan);
            }


            globalExecutionPlan->addExecutionNode(nodeEx);
                for(auto& highestCostNodeParent : topology->findNodeWithId(highestEffectiveCostNode->getId())->getParents()){
                    if(globalExecutionPlan->checkIfExecutionNodeExists(highestCostNodeParent->as<TopologyNode>()->getId())){
                        for(auto& highestCostNodeParent : topology->findNodeWithId(highestEffectiveCostNode->getId())->getParents()){

                        globalExecutionPlan->getExecutionNodeByNodeId(
                                               highestCostNodeParent->as<TopologyNode>()->getId())->addChild(nodeEx);
                    }
                }
                for(auto& highestCostNodeChild : highestEffectiveCostNode->getChildren()){
                    if(globalExecutionPlan->checkIfExecutionNodeExists(highestCostNodeChild->as<TopologyNode>()->getId())){
                        OperatorNodePtr oppy = highestEffectiveCostNode->getQuerySubPlans(queryId)[0]->getRootOperators()[0];
                        ExecutionNodePtr nodeEx = ExecutionNode::createExecutionNode(topNode,queryId,oppy);

                        topology->findNodeWithId(nodeEx->getId())->addNodeProperty("ressources",tuple);

                        globalExecutionPlan->getExecutionNodeByNodeId(
                                               highestCostNodeChild->as<TopologyNode>()->getId())->addParent(nodeEx);
                    }
                }
        }
        float localExecutionPlanCost = calcExecutionPlanCosts(globalExecutionPlan);

        if(localExecutionPlanCost > highestExecutionPlanCost){
            highestExecutionPlanCost = localExecutionPlanCost;
        }

        globalExecutionPlan->removeExecutionNode(nodeEx->getId());
    }
    }*/

    /*NES_INFO("HIGHEST ACTIVE STANDBY COST: " + std::to_string(highestExecutionPlanCost));


    //Print results.
    if (activeStandbyPossible) {
        NES_WARNING("\n[CPU] ACTIVE_STANDBY POSSIBLE [" + std::to_string(activeStandbyCandidateIds.size()) + " candidate Node(s)]. CPU_WEIGHT = "
                    + std::to_string(highestCost));
        return FaultToleranceType::ACTIVE_STANDBY;
    } else {
        NES_WARNING("\n[CPU] ACTIVE_STANDBY NOT POSSIBLE. WEIGHT OF " + std::to_string(highestCost) + " CANNOT BE SUPPORTED. ");
    }

    //Print results.
    if (checkpointingPossible) {
        NES_WARNING("\n[CPU] CHECKPOINTING POSSIBLE [" + std::to_string(checkpointingCandidateIds.size()) + " candidate Node(s)]. CPU_WEIGHT = 0.");
        return FaultToleranceType::CHECKPOINTING;
    } else {
        NES_WARNING("\n[CPU] CHECKPOINTING NOT POSSIBLE. NO CANDIDATE NODES WERE FOUND. ");
    }





    return FaultToleranceType::NONE;

}


bool BottomUpStrategy::otherNodesAvailable(GlobalExecutionPlanPtr globalExecutionPlan, std::vector<long> topologyIds, QueryId queryId) {

    bool othersAvailable = false;
    std::vector<long> executionIds = std::vector<long>();

    for(auto& node : globalExecutionPlan->getExecutionNodesByQueryId(queryId)){
        executionIds.push_back(node->getId());
    }

    for (auto& id : topologyIds) {

        if (!(std::find(executionIds.begin(),
                        executionIds.end(),
                        id)
              != executionIds.end())) {
            return true;
        }
    }

    return othersAvailable;
}

uint16_t BottomUpStrategy::getEffectivelyUsedResources(ExecutionNodePtr exNode, TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan, QueryId queryId) const {

    int highestChildCapacity = 0;
    int childCost = 0;


    if(topology->findNodeWithId(exNode->getId())->getChildren().empty()){
        NES_INFO("This node has no children.")
        return exNode->getOccupiedResources(queryId);
    }

    for (auto& child : topology->findNodeWithId(exNode->getId())->getChildren()){

        int childId = child->as<TopologyNode>()->getId();



        if (globalExecutionPlan->checkIfExecutionNodeExists(childId) && topology->findNodeWithId(child->as<TopologyNode>()->getId())->getResourceCapacity() >= highestChildCapacity
            && globalExecutionPlan->getExecutionNodeByNodeId(child->as<TopologyNode>()->getId())->getOccupiedResources(queryId) > 0
            ){


            ExecutionNodePtr childExNode = globalExecutionPlan->getExecutionNodeByNodeId(childId);
            int childExNodeCost = childExNode->getOccupiedResources(queryId);

            highestChildCapacity = child->as<TopologyNode>()->getResourceCapacity();
            childCost = childExNodeCost;
        }else{
            return exNode->getOccupiedResources(queryId);
        }
    }
    NES_INFO("HIGHEST CHILD CAP: " + std::to_string(highestChildCapacity) + ", RELATION: " + std::to_string(highestChildCapacity) + "/"
             + std::to_string(exNode->as<TopologyNode>()->getResourceCapacity()) + " = " + std::to_string(highestChildCapacity / exNode->as<TopologyNode>()->getResourceCapacity()))
    return (childCost * (highestChildCapacity / exNode->as<TopologyNode>()->getResourceCapacity()));

}

float BottomUpStrategy::calcExecutionPlanCosts(GlobalExecutionPlanPtr globalExecutionPlan){

    float cost = 0;

    for(auto& enode : globalExecutionPlan->getAllExecutionNodes()){
        TopologyNodePtr topNode = topology->findNodeWithId(enode->getId());

        cost += std::get<1>(std::any_cast<std::tuple<int,float>>(topNode->getNodeProperty("ressources")));
        cost += std::get<1>(std::any_cast<std::tuple<float,float>>(topNode->getNodeProperty("latency")));
    }

    return cost;
}*/

/*
void BottomUpStrategy::calcEffectiveValues(std::vector<TopologyNodePtr> topologyNodes, QueryId queryId){

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::queue<TopologyNodePtr> q;
    std::vector<TopologyNodePtr> answer;

    q.push(topology->getRoot());

    while(!q.empty()){
        TopologyNodePtr first = q.front();
        answer.push_back(first);
        q.pop();

        for(auto& child : first->getChildren()){
            q.push(child->as<TopologyNode>());
        }
    }



    for(auto& node : answer){

        if(globalExecutionPlan->checkIfExecutionNodeExists(node->getId())){

            ExecutionNodePtr nodeEx = globalExecutionPlan->getExecutionNodeByNodeId(node->getId());

            if(!node->getChildren().empty()){
                for(auto& child : node->getChildren()){

                    TopologyNodePtr childTop = child->as<TopologyNode>();

                    if(globalExecutionPlan->checkIfExecutionNodeExists(childTop->getId())){

                        ExecutionNodePtr childEx = globalExecutionPlan->getExecutionNodeByNodeId(childTop->getId());

                        float childExOccupiedResources = childEx->getOccupiedResources(queryId);
                        float nodeResourceCapacity = node->getResourceCapacity();
                        float childExResourceCapacity = childEx->getTopologyNode()->getResourceCapacity();
                        float resourceCapacityRatio =  nodeResourceCapacity / childExResourceCapacity;

                        float childEffectiveCost = nodeEx->getOccupiedResources(queryId) * resourceCapacityRatio;

                        if(childEffectiveCost > childTop->getEffectiveRessources()){
                            childTop->setEffectiveRessources(childEffectiveCost);
                            childTop->addNodeProperty("ressources",std::tuple<int,float>{childEx->getOccupiedResources(queryId),
                                                                                           childEffectiveCost});
                        }
                    }
                }
            }
            if(node->getParents().empty()){
                node->addNodeProperty("ressources",std::tuple<int,float>{nodeEx->getOccupiedResources(queryId),
                                                                           nodeEx->getOccupiedResources(queryId)});
                node->setEffectiveRessources(nodeEx->getOccupiedResources(queryId));
            }
        }
    }

    std::reverse(begin(answer),end(answer));

    auto workerRpcClient = std::make_shared<WorkerRPCClient>();


    for(auto& topNode : topologyNodes){


        auto ipAddress = topNode->getIpAddress();
        auto grpcPort = topNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);

        int localLatency = workerRpcClient->getRTT(rpcAddress);

        if(localLatency != (-1)){
            topNode->setLatency(localLatency);
        }else{
            topNode->setLatency(rand() % 500 + 1);
        }
    }

    //prev working: for(auto& enode: executionNodes){
    for(auto& enode: globalExecutionPlan->getExecutionNodesByQueryId(queryId)){

        TopologyNodePtr node = topology->findNodeWithId(enode->getId());

        auto ipAddress = node->getIpAddress();
        auto grpcPort = node->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);

        //TODO implement latency restraints
        int localAvailableBuffers = workerRpcClient->sayHi(queryId, rpcAddress);


        if(localAvailableBuffers != (-1)){
            node->setAvailableBuffers(localAvailableBuffers);
        }else{
            node->setAvailableBuffers(rand() % 2000 + 500);
        }
    }


    for (auto& node : answer){

        TopologyNodePtr topNode = node->as<TopologyNode>();

        if(node->getChildren().empty() && globalExecutionPlan->checkIfExecutionNodeExists(topNode->getId())){
            topNode->setEffectiveLatency(node->as<TopologyNode>()->getLatency());
            topNode->addNodeProperty(std::to_string(queryId),std::tuple<float,float>{topNode->getLatency(),topNode->getLatency()});
        }

        if(!node->getParents().empty()){
            for (auto& parent : node->getParents()){

                bool hasExNodeChild = false;

                for(auto& child : parent->getChildren()){
                    TopologyNodePtr childTop = child->as<TopologyNode>();
                    if(globalExecutionPlan->checkIfExecutionNodeExists(childTop->getId())){
                        hasExNodeChild = true;
                        break;
                    }
                }

                TopologyNodePtr parentTop = parent->as<TopologyNode>();

                float parentTopLatency = static_cast<float>(parentTop->getLatency());
                float floatSize = static_cast<float>(globalExecutionPlan->getAllExecutionNodes().size());
                float latencyRatio = static_cast<float>(1.0 / (floatSize));
                float newEffectiveLatency = parentTopLatency * latencyRatio;

                if(globalExecutionPlan->checkIfExecutionNodeExists(topNode->getId())){
                    if(globalExecutionPlan->checkIfExecutionNodeExists(parentTop->getId())){


                        if(newEffectiveLatency > parentTop->getEffectiveLatency()){
                            parentTop->setEffectiveLatency(newEffectiveLatency);
                            parentTop->removeNodeProperty("latency");
                            parentTop->addNodeProperty("latency",std::tuple<float,float>{parentTop->getLatency(),newEffectiveLatency});
                        }
                    }
                }
                else{
                    if(globalExecutionPlan->checkIfExecutionNodeExists(parentTop->getId())){
                        if(!hasExNodeChild){
                            parentTop->setEffectiveLatency(parentTop->getLatency());
                            parentTop->removeNodeProperty("latency");
                            parentTop->addNodeProperty("latency",std::tuple<float,float>{parentTop->getLatency(),parentTop->getLatency()});
                        }
                    }
                }
            }
        }
    }

    for(auto& exNode : executionNodes){
        TopologyNodePtr topNode = topology->findNodeWithId(exNode->getId());
        for(auto& exNodeParent : topNode->getParents()){
            if(globalExecutionPlan->getExecutionNodeByNodeId(topology->findNodeWithId(exNodeParent->as<TopologyNode>()->getId())->getId())->hasQuerySubPlans(queryId)){
                TopologyNodePtr topNodeParent = exNodeParent->as<TopologyNode>();
                auto startIpAddress = topNode->getIpAddress();
                auto startGrpcPort = topNode->getGrpcPort();
                std::string startRpcAddress = startIpAddress + ":" + std::to_string(startGrpcPort);

                auto destIpAddress = topNodeParent->getIpAddress();
                auto destGrpcPort = topNodeParent->getGrpcPort();
                std::string destRpcAddress = destIpAddress + ":" + std::to_string(destGrpcPort);

                int conn = workerRpcClient->getConnectivity(startRpcAddress,destRpcAddress);

                if(conn == -1){
                    conn = (rand() % 100 + 10);
                }

                NES_INFO("NODE#" + std::to_string(exNode->getId()) + " TO NODE#" + std::to_string(exNodeParent->as<TopologyNode>()->getId())
                         + " HAS CONN OF: " + std::to_string(conn));

                if(topNode->hasNodeProperty("connectivity")){
                    std::map<int,int> linkConnecitivites = std::any_cast<std::map<int,int>>(topNode->getNodeProperty("connectivity"));

                    linkConnecitivites.insert({topNodeParent->getId(),conn});
                    topNode->removeNodeProperty("connectivity");
                    topNode->addNodeProperty("connectivity",(topNodeParent->getId(),linkConnecitivites));
                }else{
                    std::map<int,int> map = {{topNodeParent->getId(),conn}};
                    topNode->addNodeProperty("connectivity",map);
                }




            }
        }



    }



}

std::tuple<float,float,float> BottomUpStrategy::calcCheckpointing(ExecutionNodePtr executionNode){
    ExecutionNodePtr eNode = executionNode;
    return {0,0,0};
}

int BottomUpStrategy::getDepth(GlobalExecutionPlanPtr globalExecutionPlan, ExecutionNodePtr executionNode, int counter){


    for(auto& parent : executionNode->getParents()){
        TopologyNodePtr topParent = parent->as<TopologyNode>();

        counter++;
        if(std::find(globalExecutionPlan->getRootNodes().begin(), globalExecutionPlan->getRootNodes().end(), parent->as<ExecutionNode>())
            != globalExecutionPlan->getRootNodes().end()){
            return counter;
        }else{
            for(auto& parent : executionNode->getParents()){
                ExecutionNodePtr exParent = globalExecutionPlan->getExecutionNodeByNodeId(parent->as<TopologyNode>()->getId());
                return getDepth(globalExecutionPlan, exParent, counter);
            }
        }
    }

    return 0;
}

std::vector<ExecutionNodePtr> BottomUpStrategy::getNeighborNodes(ExecutionNodePtr executionNode, int levelsLower, int targetDepth){

    std::vector<ExecutionNodePtr> answer;

    if(targetDepth - levelsLower == 0){
        for(auto& child : executionNode->getChildren()){
            answer.push_back(child->as<ExecutionNode>());
        }
    }else{
        for(auto& child : executionNode->getChildren()){
            std::vector<ExecutionNodePtr> answer2 = getNeighborNodes(child->as<ExecutionNode>(), levelsLower + 1, targetDepth);
            answer.insert(answer.end(), answer2.begin(), answer2.end());
        }
    }

    return answer;

}

std::tuple<float,float,float> BottomUpStrategy::calcActiveStandby(ExecutionNodePtr executionNode, int replicas, QueryId queryId){
    TopologyNodePtr topNode = topology->findNodeWithId(executionNode->getId());

    QueryId q = queryId;

    float procCost = 0;

    std::vector<TopologyNodePtr> executionNodeParents;
    std::vector<TopologyNodePtr> usedParents;

    //TODO determine how to choose which downstream nodes to use as replicas.
    for(auto& parent : topNode->getParents()){
        TopologyNodePtr parentTop = parent->as<TopologyNode>();
        if(globalExecutionPlan->checkIfExecutionNodeExists(parentTop->getId())){
            executionNodeParents.push_back(parentTop);
        }
    }

    if(executionNodeParents.size() < static_cast<size_t>(replicas)){
        return {-1,-1,-1};
    }else{
        for (int i = 0; i < replicas; ++i) {
            usedParents.push_back(executionNodeParents[i]);
            procCost += getEffectiveProcessingCosts(globalExecutionPlan->getExecutionNodeByNodeId(executionNodeParents[i]->getId()));
        }
    }

    float networkCost = 0;

    float memoryCost = calcOutputQueue(executionNode);

    for(auto& replica : usedParents){
        networkCost += calcLinkWeight(executionNode, globalExecutionPlan->getExecutionNodeByNodeId(replica->getId()));
        memoryCost += calcOutputQueue(globalExecutionPlan->getExecutionNodeByNodeId(replica->getId()));
    }

    return {procCost,networkCost,memoryCost};
}*/

/*float BottomUpStrategy::calcUpstreamBackup(ExecutionNodePtr executionNode, QueryId queryId){
    TopologyNodePtr topNode = topology->findNodeWithId(executionNode->getId());

    float procCost = getEffectiveProcessingCosts(executionNode);

    float networkCost = 0;

    //TODO determine which node to use as primary and which one to use as secondary
    networkCost += calcLinkWeight(executionNode, primary);
    networkCost += calcLinkWeight(executionNode, secondary);
    for(auto& upstreamNode : getDownstreamTree(primary, false)){
        networkCost += calcDownstreamLinkWeights(upstreamNode, queryId);
    }
    for(auto& upstreamNode : getDownstreamTree(secondary, false)){
        networkCost += calcDownstreamLinkWeights(upstreamNode, queryId);
    }

    float memoryCost = calcOutputQueue(executionNode);


    return 0;

}*/

/*
float BottomUpStrategy::calcDownstreamLinkWeights(ExecutionNodePtr executionNode, QueryId queryId){
    TopologyNodePtr topNode = topology->findNodeWithId(executionNode->getId());

    float downstreamLinkWeights = 0;

    for(auto& parent : topNode->getParents()){
        TopologyNodePtr topParent = parent->as<TopologyNode>();
        ExecutionNodePtr exParent = globalExecutionPlan->getExecutionNodeByNodeId(topParent->getId());
        if(globalExecutionPlan->checkIfExecutionNodeExists(topParent->getId()) && exParent->hasQuerySubPlans(queryId)){
            downstreamLinkWeights += calcLinkWeight(executionNode, exParent);
        }
    }

    return downstreamLinkWeights;
}

std::vector<TopologyNodePtr> BottomUpStrategy::getUpstreamTree(TopologyNodePtr topNode, bool reversed){

    std::queue<TopologyNodePtr> q;
    std::vector<TopologyNodePtr> answer;

    q.push(topNode);

    while(!q.empty()){
        TopologyNodePtr first = q.front();
        answer.push_back(first);
        q.pop();

        for(auto& child : first->getChildren()){
            q.push(child->as<TopologyNode>());
        }
    }

    if(reversed){
        std::reverse(begin(answer),end(answer));
    }
    return answer;
}

std::vector<TopologyNodePtr> BottomUpStrategy::getDownstreamTree(TopologyNodePtr topNode, bool reversed){

    std::queue<TopologyNodePtr> q;
    std::vector<TopologyNodePtr> answer;

    q.push(topNode);

    while(!q.empty()){
        TopologyNodePtr first = q.front();
        answer.push_back(first);
        q.pop();

        for(auto& parent : first->getParents()){
            q.push(parent->as<TopologyNode>());
        }
    }

    if(reversed){
        std::reverse(begin(answer),end(answer));
    }
    return answer;
}

float BottomUpStrategy::calcNetworkingCosts(GlobalExecutionPlanPtr globalExecutionPlan, QueryId queryId, ExecutionNodePtr executionNode){
    TopologyNodePtr topNode = topology->findNodeWithId(executionNode->getId());

    float cost = 0;

    for(auto& parent : topNode->getParents()){
        TopologyNodePtr topParent = parent->as<TopologyNode>();
        ExecutionNodePtr exParent = globalExecutionPlan->getExecutionNodeByNodeId(topParent->getId());
        if(globalExecutionPlan->checkIfExecutionNodeExists(topParent->getId()) && exParent->hasQuerySubPlans(queryId)){
            cost += calcLinkWeight(executionNode, exParent);
        }
    }

    return cost;
}

float BottomUpStrategy::calcLinkWeight(ExecutionNodePtr upstreamNode, ExecutionNodePtr downstreamNode){
    TopologyNodePtr topUpstreamNode = topology->findNodeWithId(upstreamNode->getId());
    TopologyNodePtr topDownstreamNode = topology->findNodeWithId(downstreamNode->getId());


    float weight =(calcOutputQueue(upstreamNode) - calcOutputQueue(downstreamNode))
        * getNetworkConnectivity(upstreamNode,downstreamNode) * topDownstreamNode->getResourceCapacity();

    return weight;
}

float BottomUpStrategy::getNetworkConnectivity(ExecutionNodePtr upstreamNode, ExecutionNodePtr downstreamNode){
    TopologyNodePtr topUpstreamNode = topology->findNodeWithId(upstreamNode->getId());
    TopologyNodePtr topDownstreamNode = topology->findNodeWithId(downstreamNode->getId());

    int conn = std::any_cast<std::map<int,int>>(topUpstreamNode->getNodeProperty("connectivity")).at(downstreamNode->getId());
    return conn;
}

float BottomUpStrategy::calcOutputQueue(ExecutionNodePtr executionNode){
    TopologyNodePtr topNode = topology->findNodeWithId(executionNode->getId());

    //TODO get queue-trimming / checkpointing interval
    //TODO get number of stateful operators within one ExecutionNode

    float interval = (rand() % 3 + 0.8);
    float statefulOperators = 0;

    for(auto& subPlanMap : executionNode->getAllQuerySubPlans()){
        for (auto& subPlan : subPlanMap.second){
            for(auto& op : subPlan->getRootOperators()){
                statefulOperators += 1;
            }
        }
    }

    float queueSize = (topNode->getAvailableBuffers() / (statefulOperators + 1)) / interval;
    return queueSize;
}

float BottomUpStrategy::getEffectiveProcessingCosts(ExecutionNodePtr executionNode){
    TopologyNodePtr topNode = topology->findNodeWithId(executionNode->getId());

    return std::get<1>(std::any_cast<std::tuple<int,float>>(topNode->getNodeProperty("ressources")));
}

*/

}// namespace NES::Optimizer
