#include <Optimizer/impl/HighAvailability.hpp>
#include <Operators/Operator.hpp>
#include <Util/Logger.hpp>
#include <Optimizer/utils/PathFinder.hpp>

namespace NES {

NESExecutionPlanPtr HighAvailability::initializeExecutionPlan(InputQueryPtr inputQuery,
                                                              NESTopologyPlanPtr nesTopologyPlan) {
    const OperatorPtr sinkOperator = inputQuery->getRoot();

    // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
    const string& streamName = inputQuery->getSourceStream()->getName();
    const OperatorPtr sourceOperatorPtr = getSourceOperator(sinkOperator);

    if (!sourceOperatorPtr) {
        NES_ERROR("HighAvailability: Unable to find the source operator.");
        throw std::runtime_error("No source operator found in the query plan");
    }

    const vector<NESTopologyEntryPtr> sourceNodePtrs = StreamCatalog::instance()
        .getSourceNodesForLogicalStream(streamName);

    if (sourceNodePtrs.empty()) {
        NES_ERROR("HighAvailability: Unable to find the target source: " << streamName);
        throw std::runtime_error("No source found in the topology for stream " + streamName);
    }

    NESExecutionPlanPtr nesExecutionPlanPtr = std::make_shared<NESExecutionPlan>();
    const NESTopologyGraphPtr nesTopologyGraphPtr = nesTopologyPlan->getNESTopologyGraph();

    NES_INFO("HighAvailability: Placing operators on the nes topology.");
    placeOperators(nesExecutionPlanPtr, nesTopologyGraphPtr, sourceOperatorPtr, sourceNodePtrs);

    NES_INFO("HighAvailability: Adding forward operators.");
    addForwardOperators(sourceNodePtrs, nesTopologyGraphPtr->getRoot(), nesExecutionPlanPtr);

    NES_INFO("HighAvailability: Generating complete execution Graph.");
    completeExecutionGraphWithNESTopology(nesExecutionPlanPtr, nesTopologyPlan);

    //FIXME: We are assuming that throughout the pipeline the schema would not change.
    Schema& schema = inputQuery->getSourceStream()->getSchema();
    addSystemGeneratedSourceSinkOperators(schema, nesExecutionPlanPtr);

    return nesExecutionPlanPtr;
}

void HighAvailability::placeOperators(NES::NESExecutionPlanPtr executionPlanPtr,
                                      NES::NESTopologyGraphPtr nesTopologyGraphPtr,
                                      NES::OperatorPtr sourceOperator,
                                      vector<NESTopologyEntryPtr> sourceNodes) {

    NESTopologyEntryPtr sinkNode = nesTopologyGraphPtr->getRoot();
    PathFinder pathFinder;
    for (NESTopologyEntryPtr sourceNode: sourceNodes) {
        vector<vector<NESTopologyEntryPtr>> allPaths = pathFinder.findAllPathsBetween(sourceNode, sinkNode);

        //Find the most common path among the list of paths



    }
}

void HighAvailability::addForwardOperators(const vector<NESTopologyEntryPtr> sourceNodes,
                                           const NES::NESTopologyEntryPtr rootNode,
                                           NES::NESExecutionPlanPtr nesExecutionPlanPtr) const {

}

}
