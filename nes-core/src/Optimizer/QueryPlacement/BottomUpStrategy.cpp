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
        faultToleranceType = checkFaultTolerance(globalExecutionPlan, topology, queryId);
        NES_INFO("\nCHOSEN FAULT-TOLERANCE APPROACH: " + toString(faultToleranceType));

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

FaultToleranceType BottomUpStrategy::checkFaultTolerance(GlobalExecutionPlanPtr globalExecutionPlan,
                                              TopologyPtr topology,
                                              QueryId queryId) {

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





    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    double totalQueryCost = 0;
    double operatorCount = 0;
    double avgOperatorCost = 0;

    double nodesCount = 0;
    double totalAvailableResources = 0;
    double totalUsedResources = 0;
    double totalResourceCapacity = 0;

    /*if(!otherNodesAvailable(globalExecutionPlan, topologyIds,queryId)){
        NES_WARNING("\nFAULT-TOLERANCE CANNOT BE PROVIDED. THERE ARE NO POTENTIAL BACKUP NODES AVAILABLE.");
        return FaultToleranceType::NONE;
    }*/


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


    //Check if there are nodes in the topology that can support the maximum query fragment cost. If there are such nodes, choose active standby.
    //TODO Potentially change the if condition so that only nodes are considered on which there is no fragment of the same query already running.
    //TODO Only consider nodes of the same depth
    for (auto& topologyId : topologyIds) {



        if (topology->findNodeWithId(topologyId)->getAvailableResources() < highestCost /*&& (!globalExecutionPlan->checkIfExecutionNodeExists(topologyNode->getId()))*/) {
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

        if (topology->findNodeWithId(topologyId)->getResourceCapacity() < highestCost /*&& (!globalExecutionPlan->checkIfExecutionNodeExists(topologyNode->getId()))*/) {
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


    NES_WARNING("sayHi1")





    /*if(success){
        NES_INFO("sayHiTrue");
    }else{
        NES_INFO("sayHiFalse")
    }*/



    //NES_INFO("top size: " + std::to_string(executionNodes.size()) + "\n ipaddress: " + rpcAddress);




    //workerClient->sayHi();
    std::vector<int> seenIds;

    /*if(calcEffectiveRessources(topologyNodes,seenIds,queryId)){
        NES_INFO("WORKY")
    }*/

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

    int ansSize = answer.size();

    for (int i = 0; i < ansSize; ++i) {
        NES_INFO("YEE")
        NES_INFO(std::to_string(i) + ": " + answer.at(i)->toString());
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
                        }
                    }
                }
            }
            if(node->getParents().empty()){
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

    for(auto& enode: executionNodes){

        TopologyNodePtr node = topology->findNodeWithId(enode->getId());

        auto ipAddress = node->getIpAddress();
        auto grpcPort = node->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_INFO("ADDY: " + rpcAddress);

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
                float latencyRatio = static_cast<float>(1.0 / floatSize);
                float newEffectiveLatency = parentTopLatency * latencyRatio;

                if(globalExecutionPlan->checkIfExecutionNodeExists(topNode->getId())){
                    if(globalExecutionPlan->checkIfExecutionNodeExists(parentTop->getId())){


                        if(newEffectiveLatency > parentTop->getEffectiveLatency()){
                            parentTop->setEffectiveLatency(newEffectiveLatency);
                        }
                    }
                }
                else{
                    if(globalExecutionPlan->checkIfExecutionNodeExists(parentTop->getId())){
                        if(!hasExNodeChild){
                            parentTop->setEffectiveLatency(parentTop->getLatency());
                        }
                    }
                }

                /*if(!globalExecutionPlan->checkIfExecutionNodeExists(topNode->getId())){
               //     parentTop->setEffectiveLatency(parentTop->getLatency());
                }else{
                    if(globalExecutionPlan->checkIfExecutionNodeExists(parentTop->getId())){

                        float parentTopLatency = static_cast<float>(parentTop->getLatency());
                        float floatSize = static_cast<float>(globalExecutionPlan->getAllExecutionNodes().size());
                        float latencyRatio = static_cast<float>(1.0 / floatSize);
                        float newEffectiveLatency = parentTopLatency * latencyRatio;

                        if(newEffectiveLatency > parentTop->getEffectiveLatency()){
                            parentTop->setEffectiveLatency(newEffectiveLatency);
                        }
                    }
                }*/
            }
        }
    }


    //std::reverse(begin(answer),end(answer));

    /*for (auto& node : answer){
        if(globalExecutionPlan->checkIfExecutionNodeExists(node->getId())){
            if(node->getChildren().empty()){
                node->setEffectiveRessources(globalExecutionPlan->getExecutionNodeByNodeId(node->getId())->getOccupiedResources(queryId));
            }else{
                for(auto& child : node->getChildren()){
                    bool hasExNodeChild = false;
                    TopologyNodePtr childTop = child->as<TopologyNode>();
                    if(globalExecutionPlan->checkIfExecutionNodeExists(childTop->getId())){
                            hasExNodeChild = true;
                        }
                    if(!hasExNodeChild){
                        node->setEffectiveRessources(globalExecutionPlan->getExecutionNodeByNodeId(node->getId())->getOccupiedResources(queryId));
                    }
                }
            }
        }
    }*/


/*
    for (auto& node : answer){

        TopologyNodePtr nodeTop = node->as<TopologyNode>();

        if(globalExecutionPlan->checkIfExecutionNodeExists(nodeTop->getId())){

            for (auto& parent : node->getParents()){

                TopologyNodePtr parentTop = parent->as<TopologyNode>();

                if(globalExecutionPlan->checkIfExecutionNodeExists(parentTop->getId())){

                    ExecutionNodePtr parentEx = globalExecutionPlan->getExecutionNodeByNodeId(parentTop->getId());

                    uint64_t parentEffectiveCost = parentEx->getOccupiedResources(queryId) * (node->getResourceCapacity() / parentEx->getTopologyNode()->getResourceCapacity());

                    if(parentEffectiveCost > parentTop->getEffectiveRessources()){
                        parentTop->setEffectiveRessources(parentEffectiveCost);
                    }
                }

            }

        }


    }*/


    NES_INFO(topology->toString())
    //===========================



    for(auto& rootNode : globalExecutionPlan->getRootNodes()){
        NES_INFO("ROOTY: " + rootNode->toString())

    }

    NES_INFO("ROOTY2: " + topology->getRoot()->toString())


    for(auto& topNode : topologyNodes){
        NES_INFO("\nLatency on #" + std::to_string(topology->findNodeWithId(topNode->getId())->getId()) + " : " +
                 std::to_string(topology->findNodeWithId(topNode->getId())->getLatency()));
    }

    for(auto& enode : executionNodes){

        TopologyNodePtr node = topology->findNodeWithId(enode->getId());

        NES_INFO("\neffyResources on #" + std::to_string(topology->findNodeWithId(enode->getId())->getId()) + " : " +
                     std::to_string(topology->findNodeWithId(enode->getId())->getEffectiveRessources()));

        NES_INFO("\neffy Latency on #" + std::to_string(topology->findNodeWithId(enode->getId())->getId()) + " : " +
                 std::to_string(topology->findNodeWithId(enode->getId())->getEffectiveLatency()));

        NES_INFO("EXXY: " + std::to_string(enode->getId()));



        NES_INFO("CHILDY: " + std::to_string(topology->findNodeWithId(1)->getChildren().size()))
        //NES_INFO("ACCY OF NODE#" + std::to_string(node->getId()) + ": " + std::to_string(getEffectivelyUsedResources(enode,topology,globalExecutionPlan,queryId)));

        NES_INFO("\nAvailable buffers: " + std::to_string(node->getAvailableBuffers()));
    }

    NES_INFO("COSTY: " + std::to_string(calcExecutionPlanCosts(globalExecutionPlan)));

    std::map<ExecutionNodePtr, int> map;
    std::vector<TopologyNodePtr> start;
    start.push_back(topology->getRoot());
    std::vector<TopologyNodePtr> end;
    end.push_back(topology->findNodeWithId(2));

    NES_INFO("SIZZY: " + std::to_string(getDepth(globalExecutionPlan->getExecutionNodeByNodeId(topology->getRoot()->getId()))));




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

/**
 * returns true if there are other nodes within the globalExecutionPlan on which the specified SubQuery is not deployed.
 * @param globalExecutionPlan
 * @param queryId
 * @return
 */
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

    //QueryId queryId1 =
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
        cost += topology->findNodeWithId(enode->getId())->getLatency();
        cost += topology->findNodeWithId(enode->getId())->getEffectiveRessources();
    }

    return cost;
}

int BottomUpStrategy::getDepth(NodePtr enode){
    if (!enode){
        return 0;
    }


    std::vector<NodePtr> nodev = enode->getChildren();

    int depth = 0;

    for(std::vector<NodePtr>::iterator it = nodev.begin(); it != nodev.end(); it++){

        auto otherDepth = getDepth(*it);

        if(otherDepth > depth){
            depth = otherDepth;
        }
    }

    return depth + 1;
}

/*std::map<ExecutionNodePtr, int> BottomUpStrategy::getNodeDepth(ExecutionNodePtr enode){

    std::map<ExecutionNodePtr, int> map;
    std::vector<TopologyNodePtr> start;
    start.push_back(topology->getRoot());
    std::vector<TopologyNodePtr> end;
    end.push_back(topology->findNodeWithId(14));

    NES_INFO("SIZZY: " + std::to_string(topology->findPathBetween(start,end).size()));

}*/
}// namespace NES::Optimizer
