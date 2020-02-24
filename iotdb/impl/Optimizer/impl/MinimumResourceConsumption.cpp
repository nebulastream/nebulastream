#include <Optimizer/NESExecutionPlan.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>
#include <Optimizer/utils/PathFinder.hpp>
#include <Operators/Operator.hpp>
#include "Optimizer/impl/MinimumResourceConsumption.hpp"

namespace NES {

NESExecutionPlanPtr MinimumResourceConsumption::initializeExecutionPlan(InputQueryPtr inputQuery,
                                                                        NESTopologyPlanPtr nesTopologyPlan) {

    const OperatorPtr sinkOperator = inputQuery->getRoot();

    // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
    const string& streamName = inputQuery->getSourceStream()->getName();
    const OperatorPtr sourceOperatorPtr = getSourceOperator(sinkOperator);

    if (!sourceOperatorPtr) {
        NES_ERROR("MinimumResourceConsumption: Unable to find the source operator.");
        throw std::runtime_error("No source operator found in the query plan");
    }

    const deque<NESTopologyEntryPtr> sourceNodePtrs = StreamCatalog::instance()
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
    addForwardOperators(sourceNodePtrs, nesTopologyGraphPtr->getRoot(), nesExecutionPlanPtr);

    NES_INFO("MinimumResourceConsumption: Removing non resident operators from the execution nodes.");
    removeNonResidentOperators(nesExecutionPlanPtr);

    NES_INFO("MinimumResourceConsumption: Generating complete execution Graph.");
    completeExecutionGraphWithNESTopology(nesExecutionPlanPtr, nesTopologyPlan);

    //FIXME: We are assuming that throughout the pipeline the schema would not change.
    Schema& schema = inputQuery->getSourceStream()->getSchema();
    addSystemGeneratedSourceSinkOperators(schema, nesExecutionPlanPtr);

    return nesExecutionPlanPtr;
}

void MinimumResourceConsumption::placeOperators(NESExecutionPlanPtr executionPlanPtr,
                                                NESTopologyGraphPtr nesTopologyGraphPtr,
                                                OperatorPtr sourceOperator,
                                                vector<NESTopologyEntryPtr> sourceNodes) {

    PathFinder pathFinder;
    const NESTopologyEntryPtr sinkNode = nesTopologyGraphPtr->getRoot();

    map<NESTopologyEntryPtr, std::vector<NESTopologyEntryPtr>>
        pathMap = pathFinder.findUniquePathBetween(sourceNodes, sinkNode);

    for (NESTopologyEntryPtr sourceNode: sourceNodes) {

        OperatorPtr targetOperator = sourceOperator;

        vector<NESTopologyEntryPtr> targetPath = pathMap[sourceNode];

        for (NESTopologyEntryPtr node : targetPath) {
            while (node->getRemainingCpuCapacity() > 0 && targetOperator) {

                if (targetOperator->getOperatorType() == SINK_OP) {
                    node = sinkNode;
                }

                if (!executionPlanPtr->hasVertex(node->getId())) {
                    NES_DEBUG("LowLatency: Create new execution node.")
                    stringstream operatorName;
                    operatorName << operatorTypeToString[targetOperator->getOperatorType()] << "(OP-"
                                 << std::to_string(targetOperator->getOperatorId()) << ")";
                    const ExecutionNodePtr newExecutionNode =
                        executionPlanPtr->createExecutionNode(operatorName.str(), to_string(node->getId()), node,
                                                              targetOperator->copy());
                    newExecutionNode->addChildOperatorId(targetOperator->getOperatorId());
                } else {

                    const ExecutionNodePtr existingExecutionNode = executionPlanPtr
                        ->getExecutionNode(node->getId());
                    size_t operatorId = targetOperator->getOperatorId();
                    vector<size_t>& residentOperatorIds = existingExecutionNode->getChildOperatorIds();
                    const auto exists = std::find(residentOperatorIds.begin(), residentOperatorIds.end(), operatorId);
                    if (exists != residentOperatorIds.end()) {
                        //skip adding rest of the operator chains as they already exists.
                        NES_DEBUG("LowLatency: skip adding rest of the operator chains as they already exists.");
                        targetOperator = nullptr;
                        break;
                    } else {

                        NES_DEBUG("LowLatency: adding target operator to already existing operator chain.");
                        stringstream operatorName;
                        operatorName << existingExecutionNode->getOperatorName() << "=>"
                                     << operatorTypeToString[targetOperator->getOperatorType()]
                                     << "(OP-" << std::to_string(targetOperator->getOperatorId()) << ")";
                        existingExecutionNode->setOperatorName(operatorName.str());
                        existingExecutionNode->addChildOperatorId(targetOperator->getOperatorId());
                    }
                }

                targetOperator = targetOperator->getParent();
                node->reduceCpuCapacity(1);
            }

            if (!targetOperator) {
                break;
            }
        }
    }

}

void MinimumResourceConsumption::addForwardOperators(vector<NESTopologyEntryPtr> sourceNodes,
                                                     NESTopologyEntryPtr rootNode,
                                                     NESExecutionPlanPtr nesExecutionPlanPtr) {
    PathFinder pathFinder;
    map<NESTopologyEntryPtr, std::vector<NESTopologyEntryPtr>>
        pathMap = pathFinder.findUniquePathBetween(sourceNodes, rootNode);

    for (NESTopologyEntryPtr targetSource: sourceNodes) {

        //Find the list of nodes connecting the source and destination nodes
        std::vector<NESTopologyEntryPtr> candidateNodes = pathMap[targetSource];

        for (NESTopologyEntryPtr candidateNode: candidateNodes) {

            if (candidateNode->getCpuCapacity() == candidateNode->getRemainingCpuCapacity()) {
                nesExecutionPlanPtr->createExecutionNode("FWD", to_string(candidateNode->getId()), candidateNode,
                    /**executableOperator**/nullptr);
                candidateNode->reduceCpuCapacity(1);
            }
        }
    }
}

}

}
