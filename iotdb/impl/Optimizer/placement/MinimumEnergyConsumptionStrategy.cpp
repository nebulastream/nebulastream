#include <Optimizer/NESExecutionPlan.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>
#include <Optimizer/utils/PathFinder.hpp>
#include <Operators/Operator.hpp>
#include "Optimizer/placement/MinimumEnergyConsumptionStrategy.hpp"

namespace NES {

NESExecutionPlanPtr MinimumEnergyConsumptionStrategy::initializeExecutionPlan(InputQueryPtr inputQuery,
                                                                      NESTopologyPlanPtr nesTopologyPlan) {

    const OperatorPtr sinkOperator = inputQuery->getRoot();

    // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
    const string& streamName = inputQuery->getSourceStream()->getName();
    const OperatorPtr sourceOperatorPtr = getSourceOperator(sinkOperator);

    if (!sourceOperatorPtr) {
        NES_ERROR("MinimumEnergyConsumption: Unable to find the source operator.");
        throw std::runtime_error("No source operator found in the query plan");
    }

    const vector<NESTopologyEntryPtr> sourceNodePtrs = StreamCatalog::instance()
        .getSourceNodesForLogicalStream(streamName);

    if (sourceNodePtrs.empty()) {
        NES_ERROR("MinimumEnergyConsumption: Unable to find the target source: " << streamName);
        throw std::runtime_error("No source found in the topology for stream " + streamName);
    }

    NESExecutionPlanPtr nesExecutionPlanPtr = std::make_shared<NESExecutionPlan>();
    const NESTopologyGraphPtr nesTopologyGraphPtr = nesTopologyPlan->getNESTopologyGraph();

    NES_INFO("MinimumEnergyConsumption: Placing operators on the nes topology.");
    placeOperators(nesExecutionPlanPtr, nesTopologyGraphPtr, sourceOperatorPtr, sourceNodePtrs);

    NES_INFO("MinimumEnergyConsumption: Adding forward operators.");
    addForwardOperators(getType(), sourceNodePtrs, nesTopologyGraphPtr->getRoot(), nesExecutionPlanPtr);

    NES_INFO("MinimumEnergyConsumption: Generating complete execution Graph.");
    completeExecutionGraphWithNESTopology(nesExecutionPlanPtr, nesTopologyPlan);

    //FIXME: We are assuming that throughout the pipeline the schema would not change.
    Schema& schema = inputQuery->getSourceStream()->getSchema();
    addSystemGeneratedSourceSinkOperators(schema, nesExecutionPlanPtr);

    return nesExecutionPlanPtr;
}

void MinimumEnergyConsumptionStrategy::placeOperators(NESExecutionPlanPtr executionPlanPtr,
                                              NESTopologyGraphPtr nesTopologyGraphPtr,
                                              OperatorPtr sourceOperator,
                                              vector<NESTopologyEntryPtr> sourceNodes) {

    const NESTopologyEntryPtr sinkNode = nesTopologyGraphPtr->getRoot();

    NES_INFO(
        "MinimumEnergyConsumption: preparing common path between sources")
    vector<NESTopologyEntryPtr> commonPath;
    PathFinder pathFinder;
    map<NESTopologyEntryPtr, std::vector<NESTopologyEntryPtr>>
        pathMap = pathFinder.findUniquePathBetween(sourceNodes, sinkNode);

    //Prepare list of ordered common nodes
    vector<vector<NESTopologyEntryPtr>> listOfPaths;
    transform(pathMap.begin(), pathMap.end(), back_inserter(listOfPaths), [](const auto pair) { return pair.second; });

    for (size_t i = 0; i < listOfPaths.size(); i++) {
        vector<NESTopologyEntryPtr> path_i = listOfPaths[i];

        for (NESTopologyEntryPtr node_i: path_i) {
            bool nodeOccursInAllPaths = false;
            for (size_t j = i; j < listOfPaths.size(); j++) {
                if (i == j) {
                    continue;
                }

                vector<NESTopologyEntryPtr> path_j = listOfPaths[j];
                const auto itr = find_if(path_j.begin(), path_j.end(), [node_i](NESTopologyEntryPtr node_j) {
                  return node_i->getId() == node_j->getId();
                });

                if (itr != path_j.end()) {
                    nodeOccursInAllPaths = true;
                } else {
                    nodeOccursInAllPaths = false;
                    break;
                }
            }

            if (nodeOccursInAllPaths) {
                commonPath.push_back(node_i);
            }
        }
    }

    NES_INFO(
        "MinimumEnergyConsumption: Sort all paths in increasing order of compute resources")
    //Sort all the paths with increased aggregated compute capacity
    vector<std::pair<size_t, int>> computeCostList;

    //Calculate total compute cost for each path
    for (size_t i = 0; i < listOfPaths.size(); i++) {
        vector<NESTopologyEntryPtr> path = listOfPaths[i];
        size_t totalComputeForPath = 0;
        for (NESTopologyEntryPtr node: path) {
            totalComputeForPath = totalComputeForPath + node->getCpuCapacity();
        }
        computeCostList.push_back(make_pair(totalComputeForPath, i));
    }

    sort(computeCostList.begin(), computeCostList.end());

    vector<vector<NESTopologyEntryPtr>> sortedListOfPaths;
    for (auto pair :  computeCostList) {
        sortedListOfPaths.push_back(listOfPaths[pair.second]);
    }

    size_t lastPlacedOperatorId;
    NES_INFO(
        "MinimumEnergyConsumption: place all non blocking operators starting from source first")
    for (auto path : sortedListOfPaths) {
        OperatorPtr targetOperator = sourceOperator;
        while (!operatorIsBlocking[targetOperator->getOperatorType()] && targetOperator->getOperatorType() != SINK_OP) {

            if (targetOperator->getOperatorType() != SOURCE_OP) {
                NES_INFO(
                    "MinimumEnergyConsumption: find if the target non blocking operator already scheduled on common path")
                bool foundOperator = false;
                for (auto commonNode : commonPath) {
                    const ExecutionNodePtr executionNode = executionPlanPtr->getExecutionNode(commonNode->getId());
                    if (executionNode) {
                        vector<size_t> scheduledOperators = executionNode->getChildOperatorIds();
                        const auto foundItr = std::find_if(scheduledOperators.begin(), scheduledOperators.end(),
                                                           [targetOperator](size_t optrId) {
                                                             return optrId == targetOperator->getOperatorId();
                                                           });
                        if (foundItr != scheduledOperators.end()) {
                            foundOperator = true;
                            break;
                        }
                    }
                }

                if (foundOperator) {
                    NES_INFO(
                        "MinimumEnergyConsumption: found target operator already scheduled.")
                    NES_INFO(
                        "MinimumEnergyConsumption: Skipping rest of the placement for current physical sensor.")
                    break;
                }
            }

            if (lastPlacedOperatorId < targetOperator->getOperatorId()) {
                lastPlacedOperatorId = targetOperator->getOperatorId();
            }

            NESTopologyEntryPtr node = nullptr;
            for (NESTopologyEntryPtr pathNode : path) {
                if (pathNode->getRemainingCpuCapacity() > 0) {
                    node = pathNode;
                    break;
                }
            }

            if (!node) {
                NES_ERROR(
                    "MinimumEnergyConsumption: Can not schedule the operator. No free resource available capacity is="
                        << sinkNode->getRemainingCpuCapacity());
                throw std::runtime_error(
                    "Can not schedule the operator. No free resource available.");
            }

            if (executionPlanPtr->hasVertex(node->getId())) {

                NES_DEBUG(
                    "MinimumEnergyConsumption: node " << node->toString()
                                                      << " was already used by other deployment")

                const ExecutionNodePtr existingExecutionNode = executionPlanPtr
                    ->getExecutionNode(node->getId());

                stringstream operatorName;
                operatorName << existingExecutionNode->getOperatorName() << "=>"
                             << operatorTypeToString[targetOperator->getOperatorType()]
                             << "(OP-" << std::to_string(targetOperator->getOperatorId()) << ")";
                existingExecutionNode->addOperator(targetOperator->copy());
                existingExecutionNode->setOperatorName(operatorName.str());
                existingExecutionNode->addOperatorId(targetOperator->getOperatorId());
            } else {

                NES_DEBUG("MinimumEnergyConsumption: create new execution node " << node->toString())

                stringstream operatorName;
                operatorName << operatorTypeToString[targetOperator->getOperatorType()] << "(OP-"
                             << std::to_string(targetOperator->getOperatorId()) << ")";

                // Create a new execution node
                const ExecutionNodePtr newExecutionNode = executionPlanPtr->createExecutionNode(operatorName.str(),
                                                                                                to_string(node->getId()),
                                                                                                node,
                                                                                                targetOperator->copy());
                newExecutionNode->addOperatorId(targetOperator->getOperatorId());
            }

            node->reduceCpuCapacity(1);
            targetOperator = targetOperator->getParent();
        }
    }

    NES_DEBUG(
        "MinimumEnergyConsumption: find the operator chain after the last placed operator");
    OperatorPtr nextSrcOptr = sourceOperator;
    while (nextSrcOptr->getOperatorId() != lastPlacedOperatorId) {
        nextSrcOptr = nextSrcOptr->getParent();
    }

    NES_DEBUG(
        "MinimumEnergyConsumption: Place remaining operator chain on common path");
    nextSrcOptr = nextSrcOptr->getParent();
    while (nextSrcOptr) {
        NESTopologyEntryPtr node = nullptr;

        if(nextSrcOptr->getOperatorType() == SINK_OP) {
            node = commonPath.back();
        } else {
            for (NESTopologyEntryPtr pathNode : commonPath) {
                if (pathNode->getRemainingCpuCapacity() > 0) {
                    node = pathNode;
                    break;
                }
            }
        }


        if (!node) {
            NES_ERROR(
                "MinimumEnergyConsumption: Can not schedule the operator. No free resource available capacity is="
                    << sinkNode->getRemainingCpuCapacity());
            throw std::runtime_error(
                "Can not schedule the operator. No free resource available.");
        }

        if (executionPlanPtr->hasVertex(node->getId())) {

            NES_DEBUG(
                "MinimumEnergyConsumption: node " << node->toString()
                                                  << " was already used by other deployment")

            const ExecutionNodePtr existingExecutionNode = executionPlanPtr
                ->getExecutionNode(node->getId());

            stringstream operatorName;
            operatorName << existingExecutionNode->getOperatorName() << "=>"
                         << operatorTypeToString[nextSrcOptr->getOperatorType()]
                         << "(OP-" << std::to_string(nextSrcOptr->getOperatorId()) << ")";
            existingExecutionNode->addOperator(nextSrcOptr->copy());
            existingExecutionNode->setOperatorName(operatorName.str());
            existingExecutionNode->addOperatorId(nextSrcOptr->getOperatorId());
        } else {

            NES_DEBUG("MinimumEnergyConsumption: create new execution node " << node->toString())

            stringstream operatorName;
            operatorName << operatorTypeToString[nextSrcOptr->getOperatorType()] << "(OP-"
                         << std::to_string(nextSrcOptr->getOperatorId()) << ")";

            // Create a new execution node
            const ExecutionNodePtr newExecutionNode = executionPlanPtr->createExecutionNode(operatorName.str(),
                                                                                            to_string(node->getId()),
                                                                                            node,
                                                                                            nextSrcOptr->copy());
            newExecutionNode->addOperatorId(nextSrcOptr->getOperatorId());
        }
        node->reduceCpuCapacity(1);
        nextSrcOptr = nextSrcOptr->getParent();
    }
}

}
