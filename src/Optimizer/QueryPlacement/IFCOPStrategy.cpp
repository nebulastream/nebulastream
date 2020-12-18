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
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
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
    ExecutionPathPtr optimizedPlan = runGlobalOptimization(queryPlan, topology, streamCatalog, maxIter);

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
ExecutionPathPtr IFCOPStrategy::runGlobalOptimization(QueryPlanPtr queryPlan, TopologyPtr topology, StreamCatalogPtr streamCatalog, int maxIter) {

    // Initiate bestCandidate and bestCost variable
    ExecutionPathPtr bestCandidate = nullptr;
    float bestCost = std::numeric_limits<float>::max();;

    // Define executionPathOptimizationMaxIter
    // TODO: take this value from the user
    int executionPathOptimizationMaxIter = 5;

    streamCatalog->getAllLogicalStream();

    for (int i=0; i<maxIter; i++) {
        // Step 1: Get an optimized execution path
        ExecutionPathPtr optimizedExecutionPath = getOptimizedExecutionPath(topology, executionPathOptimizationMaxIter, queryPlan);

        // Step 2: Get an optimized assignment
        ExecutionPathPtr optimizedExecutionPathWithAssignment = getRandomAssignment(optimizedExecutionPath, queryPlan);

        // Step 3: Evaluate cost
        float cost = getTotalCost(optimizedExecutionPathWithAssignment);
        if (cost < bestCost){
            bestCandidate = optimizedExecutionPathWithAssignment;
        }
    }

    return NES::ExecutionPathPtr();
}
float IFCOPStrategy::getTotalCost(ExecutionPathPtr executionPath) {
    executionPath.get();
    // TODO: implement IFCOP cost
    return 0;
}
ExecutionPathPtr IFCOPStrategy::getRandomAssignment(ExecutionPathPtr executionPath, QueryPlanPtr queryPlan) {
    // TODO: implement IFCOP random assignment
    queryPlan.get();
    return executionPath;
}
ExecutionPathPtr IFCOPStrategy::getOptimizedExecutionPath(TopologyPtr topology, int maxIter, QueryPlanPtr queryPlan) {
    // TODO: implement IFCOP execution path optimization
    ExecutionPathPtr bestExecutionPathCandidate = nullptr;


    for (int i=0; i<maxIter; i++) {
        TopologyNodePtr candidateExecutionPath = generateRandomExecutionPath(topology, queryPlan);

        // TODO: compute cost
    }

    return bestExecutionPathCandidate;
}

// TODO: Merge paths from multiple logical sources
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
                currentNodeOfExtendingPath = currentNodeOfExtendingPath->getChildren()[0]->as<TopologyNode>();
            } else {
                currentNodeOfExtendingPath = nullptr;
            }
        }
        NES_DEBUG("IFCOP: Finished merging one source");
    }
    return rootOfMergedPath;

}

}// namespace NES
