//
// Created by Jule on 26.05.2021.
//

#include "../../../include/Optimizer/QueryPlacement/ILPStrategy.hpp"
#include"z3++.h"
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>

namespace NES::Optimizer {

std::unique_ptr<ILPStrategy> ILPStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                           TopologyPtr topology,
                                                           TypeInferencePhasePtr typeInferencePhase,
                                                           StreamCatalogPtr streamCatalog) {
    return std::make_unique<ILPStrategy>(ILPStrategy(globalExecutionPlan, topology, typeInferencePhase, streamCatalog));
}

ILPStrategy::ILPStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                   TopologyPtr topology,
                                   TypeInferencePhasePtr typeInferencePhase,
                                   StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(globalExecutionPlan, topology, typeInferencePhase, streamCatalog) {}


bool ILPStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan){
    const QueryId queryId = queryPlan->getQueryId();
    try {
        NES_INFO("ILPStrategy: Performing placement of the input query plan with id " << queryId);
        NES_INFO("ILPStrategy: And query plan \n" << queryPlan->toString());
        NES_INFO("ILPStrategy: And topology plan \n" << topology->toString());

        std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

        NES_DEBUG("ILPStrategy: map pinned operators to the physical location");
        mapPinnedOperatorToTopologyNodes(queryId, sourceOperators);

        SourceLogicalOperatorNodePtr sourceOperator = sourceOperators.at(0);
        NES_INFO("ILPStrategy: Source Node \n" << sourceOperator->toString());

        TopologyNodePtr candidateTopologyNode = getTopologyNodeForPinnedOperator(sourceOperator->getId());

        NES_DEBUG("ILPStrategy: Candidate Node : \n" << candidateTopologyNode->toString());

        ExecutionNodePtr candidateExecutionNode = getExecutionNode(candidateTopologyNode);
        operatorToExecutionNodeMap[sourceOperator->getId()] = candidateExecutionNode;

        NES_TRACE("ILPStrategy: Update the global execution plan with candidate execution node");
        globalExecutionPlan->addExecutionNode(candidateExecutionNode);

        NES_DEBUG("ILPStrategy: Updated Global Execution Plan : \n" << globalExecutionPlan->getAsString());

    } catch (Exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }

    return true;
}

/**
 * assigns operators to topology nodes based on ILP solution
 */
void ILPStrategy::placeOperators() {

}

/**
 * assigns the output property to the operators
 * formula: output(operator) = output(parent)*dmf(operator)
 */
//void ILPStrategy::assignOperatorOutput(){}

/**
 * calculates the mileage property for a node
 * mileage: distance to the root node
 * assumes distance of 1 between each node
 * future improvement: consider link capacity
 */
//void ILPStrategy::assignNodeMilage(){}

/**
 * retrieves a property of a given operator
 */
//long ILPStrategy::getOperatorProperty(std::string property){}

/**
 * retrieves a property of a given node
 */
//long ILPStrategy::getNodeProperty(std::string property){}

}// namespace NES::Optimizer