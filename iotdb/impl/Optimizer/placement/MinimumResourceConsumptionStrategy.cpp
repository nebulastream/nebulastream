#include <Optimizer/NESExecutionPlan.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>
#include <Optimizer/utils/PathFinder.hpp>
#include <Operators/Operator.hpp>
#include <Optimizer/placement/MinimumResourceConsumptionStrategy.hpp>

namespace NES {

NESExecutionPlanPtr MinimumResourceConsumptionStrategy::initializeExecutionPlan(InputQueryPtr inputQuery,
                                                                        NESTopologyPlanPtr nesTopologyPlan) {

    const OperatorPtr sinkOperator = inputQuery->getRoot();

    // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
    const string& streamName = inputQuery->getSourceStream()->getName();
    const OperatorPtr sourceOperatorPtr = getSourceOperator(sinkOperator);

    if (!sourceOperatorPtr) {
        NES_ERROR("MinimumResourceConsumption: Unable to find the source operator.");
        throw std::runtime_error("No source operator found in the query plan");
    }

    const vector<NESTopologyEntryPtr> sourceNodePtrs = StreamCatalog::instance()
        .getSourceNodesForLogicalStream(streamName);

    if (sourceNodePtrs.empty()) {
        NES_ERROR("MinimumResourceConsumption: Unable to find the target source: " << streamName);
        throw std::runtime_error("No source found in the topology for stream " + streamName);
    }

    NESExecutionPlanPtr nesExecutionPlanPtr = std::make_shared<NESExecutionPlan>();
    const NESTopologyGraphPtr nesTopologyGraphPtr = nesTopologyPlan->getNESTopologyGraph();

    NES_INFO("MinimumResourceConsumption: Placing operators on the nes topology.");
    placeOperators(nesExecutionPlanPtr, nesTopologyGraphPtr, sourceOperatorPtr, sourceNodePtrs);

    NES_INFO("MinimumResourceConsumption: Adding forward operators.");
    addForwardOperators(getType(), sourceNodePtrs, nesTopologyGraphPtr->getRoot(), nesExecutionPlanPtr);

    NES_INFO("MinimumResourceConsumption: Generating complete execution Graph.");
    completeExecutionGraphWithNESTopology(nesExecutionPlanPtr, nesTopologyPlan);

    //FIXME: We are assuming that throughout the pipeline the schema would not change.
    Schema& schema = inputQuery->getSourceStream()->getSchema();
    addSystemGeneratedSourceSinkOperators(schema, nesExecutionPlanPtr);

    return nesExecutionPlanPtr;
}

void MinimumResourceConsumptionStrategy::placeOperators(NESExecutionPlanPtr executionPlanPtr,
                                                NESTopologyGraphPtr nesTopologyGraphPtr,
                                                OperatorPtr sourceOperator,
                                                vector<NESTopologyEntryPtr> sourceNodes) {

    PathFinder pathFinder;
    const NESTopologyEntryPtr sinkNode = nesTopologyGraphPtr->getRoot();

    map<NESTopologyEntryPtr, std::vector<NESTopologyEntryPtr>>
        pathMap = pathFinder.findUniquePathBetween(sourceNodes, sinkNode);

    //Prepare list of ordered common nodes
    vector<vector<NESTopologyEntryPtr>> listOfPaths;
    transform(pathMap.begin(), pathMap.end(), back_inserter(listOfPaths), [](const auto pair) { return pair.second; });

    vector<NESTopologyEntryPtr> commonPath;

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

    for (NESTopologyEntryPtr sourceNode: sourceNodes) {

        NES_DEBUG("MinimumResourceConsumption: Create new execution node for source operator.")
        stringstream operatorName;
        operatorName << operatorTypeToString[sourceOperator->getOperatorType()] << "(OP-"
                     << std::to_string(sourceOperator->getOperatorId()) << ")";
        const ExecutionNodePtr newExecutionNode =
            executionPlanPtr->createExecutionNode(operatorName.str(), to_string(sourceNode->getId()), sourceNode,
                                                  sourceOperator->copy());
        newExecutionNode->addOperatorId(sourceOperator->getOperatorId());
        sourceNode->reduceCpuCapacity(1);
    }

    OperatorPtr targetOperator = sourceOperator->getParent();

    while (targetOperator && targetOperator->getOperatorType() != SINK_OP) {
        NESTopologyEntryPtr node = nullptr;
        for (NESTopologyEntryPtr commonNode : commonPath) {
            if (commonNode->getRemainingCpuCapacity() > 0) {
                node = commonNode;
                break;
            }
        }

        if (!node) {
            NES_ERROR(
                "MinimumResourceConsumption: Can not schedule the operator. No free resource available capacity is="
                    << sinkNode->getRemainingCpuCapacity());
            throw std::runtime_error(
                "Can not schedule the operator. No free resource available.");
        }

        NES_DEBUG("MinimumResourceConsumption: suitable placement for operator " << targetOperator->toString() << " is "
                                                                                 << node->toString())

        // If the selected nes node was already used by another operator for placement then do not create a
        // new execution node rather add operator to existing node.
        if (executionPlanPtr->hasVertex(node->getId())) {

            NES_DEBUG(
                "MinimumResourceConsumption: node " << node->toString() << " was already used by other deployment")

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

            NES_DEBUG("MinimumResourceConsumption: create new execution node " << node->toString())

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

        // Reduce the processing capacity by 1
        // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
        node->reduceCpuCapacity(1);

        if (targetOperator->getParent() != nullptr) {
            targetOperator = targetOperator->getParent();
        }
    }

    if (sinkNode->getRemainingCpuCapacity() > 0) {
        if (executionPlanPtr->hasVertex(sinkNode->getId())) {

            NES_DEBUG(
                "MinimumResourceConsumption: node " << sinkNode->toString() << " was already used by other deployment")

            const ExecutionNodePtr existingExecutionNode = executionPlanPtr
                ->getExecutionNode(sinkNode->getId());

            stringstream operatorName;
            operatorName << existingExecutionNode->getOperatorName() << "=>"
                         << operatorTypeToString[targetOperator->getOperatorType()]
                         << "(OP-" << std::to_string(targetOperator->getOperatorId()) << ")";
            existingExecutionNode->addOperator(targetOperator->copy());
            existingExecutionNode->setOperatorName(operatorName.str());
            existingExecutionNode->addOperatorId(targetOperator->getOperatorId());
        } else {

            NES_DEBUG(
                "MinimumResourceConsumption: create new execution node " << sinkNode->toString()
                                                                         << " with sink operator")

            stringstream operatorName;
            operatorName << operatorTypeToString[targetOperator->getOperatorType()] << "(OP-"
                         << std::to_string(targetOperator->getOperatorId()) << ")";

            // Create a new execution node
            const ExecutionNodePtr newExecutionNode = executionPlanPtr->createExecutionNode(operatorName.str(),
                                                                                            to_string(sinkNode->getId()),
                                                                                            sinkNode,
                                                                                            targetOperator->copy());
            newExecutionNode->addOperatorId(targetOperator->getOperatorId());
        }
        sinkNode->reduceCpuCapacity(1);
    } else {
        NES_ERROR(
            "MinimumResourceConsumption: Can not schedule the operator. No free resource available capacity is="
                << sinkNode->getRemainingCpuCapacity());
        throw std::runtime_error(
            "Can not schedule the operator. No free resource available.");
    }

}

}
