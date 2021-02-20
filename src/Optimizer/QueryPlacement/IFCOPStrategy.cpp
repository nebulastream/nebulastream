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

#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/ExecutionPath.hpp>
#include <Optimizer/QueryPlacement/IFCOPStrategy.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <limits>
namespace NES {

std::unique_ptr<IFCOPStrategy> IFCOPStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                                           TypeInferencePhasePtr typeInferencePhase,
                                                           StreamCatalogPtr streamCatalog) {
    return std::make_unique<IFCOPStrategy>(IFCOPStrategy(globalExecutionPlan, topology, typeInferencePhase, streamCatalog));
}

IFCOPStrategy::IFCOPStrategy(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                   TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(globalExecutionPlan, topology, typeInferencePhase, streamCatalog) {}

bool IFCOPStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan) {

    const QueryId queryId = queryPlan->getQueryId();
    NES_INFO("IfcopStrategy: Performing placement of the input query plan with id " << queryId);

    // Step 1: Get the Data Modification Factor
    // get the tuple size modifier
    // TODO: Replace static tuple size modifier
    std::vector<float> tupleSizeModifier;
    for (int i=0; i<queryPlan->getLeafOperators().size();i++){
        tupleSizeModifier.push_back(1);
    }

    // get the tuple size modifier
    // TODO: Replace static ingestion rate modifier
    std::vector<float> ingestionRateModifier;
    for (int i=0; i<queryPlan->getLeafOperators().size();i++){
        ingestionRateModifier.push_back(1);
    }

    // Step 2: Get the Cost Weight
    // get the tuple size modifier
    // TODO: Replace hardcoded cost weight
    std::vector<float> costWeight;
    for (int i=0; i<2;i++){
        costWeight.push_back(0.5);
    }

    // Step 3: Get the maximum iteration
    int maxIter = 5;

    // Step 4: Initiate global optimization
    TopologyNodePtr optimizedPlan = runGlobalOptimization(queryPlan, topology, streamCatalog, maxIter);

//    NES_DEBUG("BottomUpStrategy: Get all source operators");
//    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
//    if (sourceOperators.empty()) {
//        NES_ERROR("BottomUpStrategy: No source operators found in the query plan wih id: " << queryId);
//        throw QueryPlacementException("BottomUpStrategy: No source operators found in the query plan wih id: " + queryId);
//    }
//
//    NES_DEBUG("BottomUpStrategy: map pinned operators to the physical location");
//    mapPinnedOperatorToTopologyNodes(queryId, sourceOperators);
//
//    NES_DEBUG("BottomUpStrategy: Get all sink operators");
//    const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = queryPlan->getSinkOperators();
//    if (sinkOperators.empty()) {
//        NES_ERROR("BottomUpStrategy: No sink operators found in the query plan wih id: " << queryId);
//        throw QueryPlacementException("BottomUpStrategy: No sink operators found in the query plan wih id: " + queryId);
//    }
//
//    NES_DEBUG("BottomUpStrategy: place query plan with id : " << queryId);
//    placeQueryPlanOnTopology(queryPlan);
//    NES_DEBUG("BottomUpStrategy: Add system generated operators for query with id : " << queryId);
//    addNetworkSourceAndSinkOperators(queryPlan);
//    NES_DEBUG("BottomUpStrategy: clear the temporary map : " << queryId);
//    operatorToExecutionNodeMap.clear();
//    pinnedOperatorLocationMap.clear();
//    NES_DEBUG(
//        "BottomUpStrategy: Run type inference phase for query plans in global execution plan for query with id : " << queryId);
//
//    NES_DEBUG("BottomUpStrategy: Update Global Execution Plan : \n" << globalExecutionPlan->getAsString());
    return runTypeInferencePhase(queryId);
}
TopologyNodePtr IFCOPStrategy::runGlobalOptimization(QueryPlanPtr queryPlan, TopologyPtr topology, StreamCatalogPtr streamCatalog, int maxIter) {

    // Initiate bestCandidate and bestCost variable
    TopologyNodePtr bestCandidate = nullptr;
    float bestCost = std::numeric_limits<float>::max();;

    // Define executionPathOptimizationMaxIter
    // TODO: take this value from the user
    int executionPathOptimizationMaxIter = 5;

    streamCatalog->getAllLogicalStream();

    for (int i=0; i<maxIter; i++) {
        // Step 1: Get an optimized execution path
        TopologyNodePtr optimizedExecutionPath = getOptimizedExecutionPath(topology, executionPathOptimizationMaxIter, queryPlan);

        // Step 2: Get an optimized assignment
//        TopologyNodePtr optimizedExecutionPathWithAssignment = getRandomAssignment(optimizedExecutionPath, queryPlan);

        // Step 3: Evaluate cost
//        float cost = getTotalCost(optimizedExecutionPathWithAssignment);
//        if (cost < bestCost){
//            bestCandidate = optimizedExecutionPathWithAssignment;
//        }
        NES_DEBUG("IFCOP: bestCost=" << bestCost);
    }

    return bestCandidate;
}

float IFCOPStrategy::getTotalCost(QueryPlanPtr queryPlan, TopologyPtr topology, TopologyNodePtr executionPath,
                                  std::map<TopologyNodePtr,std::vector<LogicalOperatorNodePtr>> operatorAssignment,
                                  std::map<LogicalOperatorNodePtr, float>  ingestionRateModifiers,
                                  std::map<LogicalOperatorNodePtr, float>  tupleSizeModifiers) {
    // dst, src
    std::map<OperatorNodePtr, OperatorNodePtr> queryEdges;
    OperatorNodePtr currentNode = queryPlan->getRootOperators()[0];

    // TODO: check on n-ary operator
    while (currentNode) {
        // traverse to the next node (topdown)
        if (!currentNode->getChildren().empty()){
            for (NodePtr child: currentNode->getChildren()){
                auto nextNode = child->as<LogicalOperatorNode>();
                queryEdges.insert(std::pair<OperatorNodePtr, OperatorNodePtr >(nextNode, currentNode) );
                currentNode = nextNode;
            }
        } else {
            currentNode = nullptr;
        }
    }

    bool isValid = true;
    // loop to queryEdges and get dst and src
    for (auto & queryEdge : queryEdges){

        TopologyNodePtr srcTopologyNodePtr = nullptr;
        TopologyNodePtr dstTopologyNodePtr = nullptr;
        for (auto &assignment: operatorAssignment) {
            if (std::find(assignment.second.begin(), assignment.second.end(), queryEdge.first)!=assignment.second.end()){
                srcTopologyNodePtr = assignment.first;
            }
            if (std::find(assignment.second.begin(), assignment.second.end(), queryEdge.second)!=assignment.second.end()){
                dstTopologyNodePtr = assignment.first;
            }
        }

        // if the operator is not placed the placement is invalid
        if (srcTopologyNodePtr == nullptr || dstTopologyNodePtr == nullptr) {
            isValid = false;
        } else {
            NES_DEBUG("op:" << queryEdge.second->toString() << " dst:" << dstTopologyNodePtr->toString() << "| op:" << queryEdge.first->toString() << " src:" << srcTopologyNodePtr->toString());
            auto isReachable = topology->checkIfReachable(srcTopologyNodePtr, dstTopologyNodePtr);
            isValid = isValid && isReachable;
        }
    }

    NES_DEBUG(topology->toString());
    NES_DEBUG(operatorAssignment.size());
    NES_DEBUG(topology->getRoot()->getId());

    // if the placement is valid, continue computing the cost
    float networkCost = std::numeric_limits<float>::max();
    if (isValid) {
        networkCost = getNetworkCost(executionPath, operatorAssignment, ingestionRateModifiers, tupleSizeModifiers);
        NES_DEBUG("IFCOP: networkCost=" << networkCost);
    }
    return networkCost;
}

float IFCOPStrategy::getNetworkCost(TopologyNodePtr currentTopologyNode,
                                    std::map<TopologyNodePtr,std::vector<LogicalOperatorNodePtr>> operatorAssignment,
                                    std::map<LogicalOperatorNodePtr, float> ingestionRateModifiers,
                                    std::map<LogicalOperatorNodePtr, float>  tupleSizeModifiers) {

    bool isSinkNode = false;
    // compute network cost in current node
    double currentNodeModifier = 1; //base modifier
    for(auto op: operatorAssignment[currentTopologyNode]){
        if (op->instanceOf<SinkLogicalOperatorNode>()){
            NES_DEBUG("IFCOP::getNetworkCost: Node=" << currentTopologyNode->getId() << " isSinkNode=True");
            isSinkNode = true;
        }
        currentNodeModifier *= ingestionRateModifiers[op] * tupleSizeModifiers[op];
    }

    if (currentTopologyNode->getChildren().size() == 0) {
        std::size_t schemaSize = operatorAssignment[currentTopologyNode].back()->as<LogicalOperatorNode>()->getOutputSchema()->getSchemaSizeInBytes();
        NES_DEBUG("CostUtil::getNetworkCost aggregated cost: " << schemaSize << " at: " << currentTopologyNode->getId());
        return schemaSize;
    } else {
        if (isSinkNode) {
            currentNodeModifier = 0;
        }
        double currentNetworkCost = 0;
        for(auto child: currentTopologyNode->getChildren()){
            double childCost = getNetworkCost(child->as<TopologyNode>(), operatorAssignment, ingestionRateModifiers, tupleSizeModifiers);
            currentNetworkCost += childCost + currentNodeModifier * childCost; // required bandwidth in bytes
        }
        NES_DEBUG("CostUtil::getNetworkCost aggregated cost: " << currentNetworkCost << " at: " << currentTopologyNode->getId());

        return currentNetworkCost;
    }
}

std::map<TopologyNodePtr,std::vector<LogicalOperatorNodePtr>> IFCOPStrategy::getRandomAssignment(TopologyNodePtr executionPath,
                                                   std::vector<SourceLogicalOperatorNodePtr> sourceOperators) {
    // Get a map of stream names and their source nodes
    std::map<std::string, std::vector<TopologyNodePtr>> mapOfSourceToTopologyNodes;
    for (auto& sourceOperator : sourceOperators) {
        const std::string streamName = sourceOperator->getSourceDescriptor()->getStreamName();
        const std::vector<TopologyNodePtr> sourceNodes = streamCatalog->getSourceNodesForLogicalStream(streamName);
        mapOfSourceToTopologyNodes[streamName] = sourceNodes;
    }
    std::map<TopologyNodePtr,std::vector<LogicalOperatorNodePtr>> nodeToOperatorsMap;
    // Place source operator to the source nodes, then trigger placement of rest of operators
    for (auto& sourceOperator : sourceOperators) {
        TopologyNodePtr candidateSourceNode = mapOfSourceToTopologyNodes[sourceOperator->getSourceDescriptor()->getStreamName()].back();
        TopologyNodePtr candidateSourceNodeInExecutionPath;

        // find matching mathcing node of candidateSourceNode in the executionPath
        for (NodePtr sourceFromExecutionPath: executionPath->getAllLeafNodes()){
            if (sourceFromExecutionPath->as<TopologyNode>()->getId() == candidateSourceNode->getId()){
                candidateSourceNodeInExecutionPath = sourceFromExecutionPath->as<TopologyNode>();
                break;
            }
        }

        mapOfSourceToTopologyNodes[sourceOperator->getSourceDescriptor()->getStreamName()].pop_back();

        if (nodeToOperatorsMap.find(candidateSourceNodeInExecutionPath) == nodeToOperatorsMap.end()){
            std::vector<LogicalOperatorNodePtr> operators = {sourceOperator};
            nodeToOperatorsMap.insert(std::pair<TopologyNodePtr, std::vector<LogicalOperatorNodePtr>>(candidateSourceNodeInExecutionPath, operators) );
        } else {
            nodeToOperatorsMap[candidateSourceNodeInExecutionPath].push_back(sourceOperator);
        }

        std::vector<NodePtr> sourceFromExecutionPath = executionPath->getAllLeafNodes();

        placeNextOperator(candidateSourceNodeInExecutionPath, sourceOperator->getParents()[0]->as<LogicalOperatorNode>(), nodeToOperatorsMap);
    }

    return nodeToOperatorsMap;
}

void IFCOPStrategy::placeNextOperator(TopologyNodePtr nextTopologyNodePtr, LogicalOperatorNodePtr nextOperatorPtr,
                                      std::map<TopologyNodePtr , std::vector<LogicalOperatorNodePtr>>& nodeToOperatorsMap) {

    // TODO: use random number generator
    uint64_t randomNumberOfOperatorToPlace = 1;
    for (int i=0; i<randomNumberOfOperatorToPlace; ++i) {
        // check if nextOperatorPtr is a sink node
        bool sinkCheck = (nextOperatorPtr->getParents().empty() && nextTopologyNodePtr->getId()
                                                                   == nextTopologyNodePtr->getAllRootNodes()[0]->as<TopologyNode>()->getId())
                         || (!nextOperatorPtr->getParents().empty());

        // if it is not a unary operator (e.g., merge) then break
        //TODO: the `mergeCheck` should check the reachability of its child operators
        uint64_t sinkNodeId = nextTopologyNodePtr->getAllRootNodes()[0]->as<TopologyNode>()->getId();
        bool mergeCheck = nextTopologyNodePtr->getId() == sinkNodeId;

        if (nextOperatorPtr->getChildren().size() > 1 && !mergeCheck) {
            break; // try to place in the parent
        }

        // place the next operator
        if (nodeToOperatorsMap.find(nextTopologyNodePtr) == nodeToOperatorsMap.end()){
            if (sinkCheck) {
                std::vector<LogicalOperatorNodePtr> operators = {nextOperatorPtr};
                nodeToOperatorsMap.insert(std::pair<TopologyNodePtr, std::vector<LogicalOperatorNodePtr>>(nextTopologyNodePtr, operators) );
            }
        } else {
            // add the operator if it has not been added before
            if (std::find(nodeToOperatorsMap[nextTopologyNodePtr].begin(), nodeToOperatorsMap[nextTopologyNodePtr].end(),
                          nextOperatorPtr) == nodeToOperatorsMap[nextTopologyNodePtr].end() && sinkCheck){
                nodeToOperatorsMap[nextTopologyNodePtr].push_back(nextOperatorPtr);
            }
        }

        if (!nextOperatorPtr->getParents().empty()){
            // get the parent of the operator we just place
            nextOperatorPtr = nextOperatorPtr->getParents()[0]->as<LogicalOperatorNode>();
        }
    }

    // check if root/sink node is reached
    if (!nextTopologyNodePtr->getParents().empty()){
        // TODO: Here we can random the index of topologyNodePtr's parent
        uint64_t topologyNodeParentIndex = 0;
        placeNextOperator(nextTopologyNodePtr->getParents()[topologyNodeParentIndex]->as<TopologyNode>(),
            nextOperatorPtr, nodeToOperatorsMap);
    } else {
        // if root/sink node is reached but there are operators left, then place the remaining operators in the sink node
        while (nextOperatorPtr){
            if (std::find(nodeToOperatorsMap[nextTopologyNodePtr].begin(), nodeToOperatorsMap[nextTopologyNodePtr].end(),
                          nextOperatorPtr) == nodeToOperatorsMap[nextTopologyNodePtr].end()){
                nodeToOperatorsMap[nextTopologyNodePtr].push_back(nextOperatorPtr);
            }
            if (nextOperatorPtr->getParents().empty()){
                nextOperatorPtr = nullptr;
            } else {
                nextOperatorPtr = nextOperatorPtr->getParents()[0]->as<LogicalOperatorNode>();
            }
        }
    }
}

TopologyNodePtr IFCOPStrategy::getOptimizedExecutionPath(TopologyPtr topology, int maxIter, QueryPlanPtr queryPlan) {
    // TODO: implement IFCOP execution path optimization
    TopologyNodePtr bestExecutionPathCandidate = nullptr;
    float bestCost = std::numeric_limits<float>::infinity();

    for (int i=0; i<maxIter; i++) {
        TopologyNodePtr candidateExecutionPath = generateRandomExecutionPath(topology, queryPlan);
        float cost = getExecutionPathCost(candidateExecutionPath);
        NES_DEBUG("IFCOP:Current cost= " << cost << " best cost= " << bestCost);
        if (cost < bestCost){
            bestCost = cost;
            bestExecutionPathCandidate = candidateExecutionPath;
        }
    }

    return bestExecutionPathCandidate;
}

TopologyNodePtr IFCOPStrategy::generateRandomExecutionPath(TopologyPtr topology, QueryPlanPtr queryPlan) {
    TopologyNodePtr sinkNode = topology->getRoot();

    // a query can have multiple sources
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

    std::vector<std::string> checkedStreamName;
    std::vector<TopologyNodePtr> rootOfExtendingPaths;
    for (auto& sourceOperator : sourceOperators) {
        const std::string streamName = sourceOperator->getSourceDescriptor()->getStreamName();

        // find possible start point
        if(std::find(checkedStreamName.begin(), checkedStreamName.end(), streamName) != checkedStreamName.end()){
            // Stream streamName already checked
            continue;
        } else {
            checkedStreamName.push_back(streamName);
        }

        const std::vector<TopologyNodePtr> sourceNodes = streamCatalog->getSourceNodesForLogicalStream(streamName);

        for (auto& sourceNode : sourceNodes) {
            const TopologyNodePtr startNode = topology->findAllPathBetween(sourceNode, {sinkNode}).value();

            TopologyNodePtr candidate = startNode->copy();
            candidate->removeAllParent();

            TopologyNodePtr childNode = startNode;
            while (childNode) {
                std::vector<NodePtr> parents = childNode->getParents();
                TopologyNodePtr selectedParent;
                if (!parents.empty()){
                    std::random_device seed;
                    std::mt19937 engine(seed()) ;
                    std::uniform_int_distribution<int> choose(0 ,parents.size() - 1) ;
                    selectedParent = parents[choose(engine)]->as<TopologyNode>();

                    TopologyNodePtr assignedParent = selectedParent->copy();
                    assignedParent->removeAllParent();
                    assignedParent->removeChildren();
                    candidate->addParent(assignedParent);
                    candidate = assignedParent;
                }
                childNode = selectedParent;
            }
            rootOfExtendingPaths.push_back(candidate);
            NES_DEBUG("IFCOP: Finished generating candidate from this source node");
        }


        NES_DEBUG("IFCOP: Finished merging all sources");
    }
    // merge as path for this logical source
    std::vector<std::string> addedIds;
    TopologyNodePtr rootOfMergedPath = rootOfExtendingPaths.at(0);
    rootOfExtendingPaths.erase(rootOfExtendingPaths.begin());

    // The following path merging algorithm assume that there will be no cycle
    // TODO: check if there is a cycle and redraw random path

    // loop over all extending nodes
    for(TopologyNodePtr rootOfExtendingPath: rootOfExtendingPaths){
        // loop over all nodes in current rootOfExtendingPath
        TopologyNodePtr currentNodeOfExtendingPath = rootOfExtendingPath;
        TopologyNodePtr currentMergedNode = rootOfMergedPath;
        TopologyNodePtr nextParent = currentMergedNode;

        // book-keep added nodes
        std::vector<uint64_t> added;
        added.push_back(currentMergedNode->getId());
        for (NodePtr childNode: currentMergedNode->getAndFlattenAllChildren(false)){
            added.push_back(childNode->as<TopologyNode>()->getId());
        }
        std::vector<uint64_t> initialMergedNodes = added;

        // loop over all nodes in the current extending path
        while(currentNodeOfExtendingPath) {
            // check if currentNodeOfExtendingPath is already added
            if (std::find(added.begin(), added.end(), currentNodeOfExtendingPath->getId()) != added.end()) {
                // update currentMergedNode to its child that has the id equal to currentNodeOfExtendingPath
                for(NodePtr childOfMergedNode : currentMergedNode->getAndFlattenAllChildren(false)){
                    if (childOfMergedNode->as<TopologyNode>()->getId() == currentNodeOfExtendingPath->getId()){
                        NES_DEBUG("IFCOP: childOfMergedNode: " << childOfMergedNode << " id: "
                                                               << childOfMergedNode->as<TopologyNode>()->getId()
                                                               << " currentNodeOfExtendingPath: "
                                                               << currentNodeOfExtendingPath  << " id: "
                                                               << currentNodeOfExtendingPath->getId());
                        currentMergedNode = childOfMergedNode->as<TopologyNode>();
                        break;
                    }
                }

                // check if we need to add a new edge to currentNodeOfExtendingPath
                if (std::find(initialMergedNodes.begin(), initialMergedNodes.end(), nextParent->getId())==initialMergedNodes.end()) {
                    nextParent->removeChildren();
                    nextParent->addChild(currentMergedNode);
                    NES_DEBUG("IFCOP: Add parent: " <<  nextParent << " with a child(currentMergedNode): " << currentMergedNode);
                }
                nextParent = currentMergedNode;

            } else {
                // add an edge to currentNodeOfExtendingPath
                nextParent->addChild(currentNodeOfExtendingPath);
                NES_DEBUG("IFCOP: Add parent: " <<  nextParent << " with a child(currentNodeOfExtendingPath): " << currentNodeOfExtendingPath);

                // register currentNodeOfExtendingPath as added
                added.push_back(currentNodeOfExtendingPath->getId());
                nextParent = currentNodeOfExtendingPath;
            }
            // traverse to the next node
            if (!currentNodeOfExtendingPath->getChildren().empty()){
                auto nextNode = currentNodeOfExtendingPath->getChildren()[0]->as<TopologyNode>();
                nextNode->removeAllParent();
                currentNodeOfExtendingPath = nextNode;
            } else {
                currentNodeOfExtendingPath = nullptr;
            }
        }
        NES_DEBUG("IFCOP: Finished merging one source");
    }
    return rootOfMergedPath;

}

// TODO: also consider bandwidth for the cost
float IFCOPStrategy::getExecutionPathCost(TopologyNodePtr executionPath) {
    uint16_t totalResources = executionPath->getAvailableResources();

    for(NodePtr child: executionPath->getAndFlattenAllChildren(false)){
        totalResources += child->as<TopologyNode>()->getAvailableResources();
    }

    return totalResources;
}

std::map<LogicalOperatorNodePtr, float> IFCOPStrategy::getTupleSizeModifier(QueryPlanPtr queryPlan) {
    std::map<LogicalOperatorNodePtr, float> tupleModifier;

    std::vector<LogicalOperatorNodePtr> nodeToVisit;

    nodeToVisit.push_back(queryPlan->getRootOperators()[0]->as<LogicalOperatorNode>());

    std::size_t schemaSize = queryPlan->getRootOperators()[0]->as<LogicalOperatorNode>()->getOutputSchema()->getSchemaSizeInBytes();
    NES_DEBUG("IFCOPStrategy::getTupleSizeModifier: schemaSize of source output:" << schemaSize);

    while (nodeToVisit.size() != 0) {
        LogicalOperatorNodePtr currentOperator = nodeToVisit.back();
        nodeToVisit.pop_back();

        if (currentOperator->getChildren().empty()){
            tupleModifier.insert(std::pair<LogicalOperatorNodePtr, float>(currentOperator, 1.0));
        } else {
            for (const auto child : currentOperator->getChildren()) {

                size_t childOutputSchemaSize = child->as<LogicalOperatorNode>()->getOutputSchema()->getSchemaSizeInBytes();
                size_t currentOperatorOutputSchemaSize = currentOperator->getOutputSchema()->getSchemaSizeInBytes();

                tupleModifier.insert(std::pair<LogicalOperatorNodePtr, float>(currentOperator, (float) currentOperatorOutputSchemaSize / childOutputSchemaSize));
                nodeToVisit.push_back(child->as<LogicalOperatorNode>());
            }
        }
    }

    NES_DEBUG("IFCOPStrategy: getTupleSizeModifier size " << tupleModifier.size());
    return tupleModifier;
}
std::map<LogicalOperatorNodePtr, float> IFCOPStrategy::getIngestionRateModifier(QueryPlanPtr queryPlan) {
    std::map<LogicalOperatorNodePtr, float> ingestionRateModifier;

    std::vector<LogicalOperatorNodePtr> nodeToVisit;

    nodeToVisit.push_back(queryPlan->getRootOperators()[0]->as<LogicalOperatorNode>());

    std::size_t schemaSize = queryPlan->getRootOperators()[0]->as<LogicalOperatorNode>()->getOutputSchema()->getSchemaSizeInBytes();
    NES_DEBUG("IFCOPStrategy::getTupleSizeModifier: schemaSize of source output:" << schemaSize);

    while (nodeToVisit.size() != 0) {
        LogicalOperatorNodePtr currentOperator = nodeToVisit.back();
        nodeToVisit.pop_back();

        if (currentOperator->getChildren().empty()){
            ingestionRateModifier.insert(std::pair<LogicalOperatorNodePtr, float>(currentOperator, 1.0));
        } else {
            for (const auto child : currentOperator->getChildren()) {
                ingestionRateModifier.insert(std::pair<LogicalOperatorNodePtr, float>(currentOperator, (float) 1.0));
                nodeToVisit.push_back(child->as<LogicalOperatorNode>());
            }
        }
    }

    NES_DEBUG("IFCOPStrategy: getTupleSizeModifier size " << ingestionRateModifier.size());
    return ingestionRateModifier;
}

}// namespace NES
