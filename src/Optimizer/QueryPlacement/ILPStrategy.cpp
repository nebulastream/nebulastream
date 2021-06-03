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

using namespace z3;

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


std::vector<NodePtr> ILPStrategy::findPathToRoot(NodePtr sourceNode){
    std::vector<NodePtr> path;
    path.push_back(sourceNode);
    while (!path.back()->getParents().empty()){
        path.push_back(path.back()->getParents()[0]); // TODO assuming single parent.
    }
    return path;
}

bool ILPStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan){
    const QueryId queryId = queryPlan->getQueryId();

    SourceLogicalOperatorNodePtr sourceOperatorNode = queryPlan->getSourceOperators()[0];
    std::string streamName = sourceOperatorNode->getSourceDescriptor()->getStreamName();
    TopologyNodePtr sourceToplogyNode = streamCatalog->getSourceNodesForLogicalStream(streamName)[0];
    std::vector<NodePtr> operatorPath = findPathToRoot(sourceOperatorNode);
    std::vector<NodePtr> topologyPath = findPathToRoot(sourceToplogyNode);

    context c;
    optimize opt(c);

    std::map<std::string, OperatorNodePtr> operatorNodes;
    std::map<std::string, TopologyNodePtr> topologyNodes;
    std::map<std::string, expr> placementVariables;
    std::map<std::string, expr> distances;

    expr cost = c.int_val(0);
    expr D_i_1 = c.int_val(0); // = D_i-1
    expr D_i = c.int_val(0);
    for (int i = 0; i < operatorPath.size(); i++) {
        OperatorNodePtr operatorNode = operatorPath[i]->as<OperatorNode>();
        std::string operatorID = std::to_string(operatorNode->getId());
        operatorNodes[operatorID] = operatorNode;
        expr sum_i = c.int_val(0);
        for (int j = 0; j < topologyPath.size(); j++) {
            TopologyNodePtr topologyNode = topologyPath[j]->as<TopologyNode>();
            std::string topologyID = std::to_string(topologyNode->getId());
            topologyNodes[topologyID] = topologyNode;

            // create placement variable and constraint to {0,1}
            std::string variableID = operatorID + ";" + topologyID;
            expr P_IJ = c.int_const(variableID.c_str());
            if (i == 0 && j == 0 || i == operatorPath.size() - 1 && j == topologyPath.size() - 1) {
                opt.add(P_IJ == 1); // fix sources and sinks
            } else {
                opt.add(P_IJ == 0 || P_IJ == 1);
            }
            placementVariables.insert(std::make_pair(variableID, P_IJ));
            sum_i = sum_i + P_IJ;

            // distance to next operator
            int M = topologyPath.size() - j - 1;
            D_i = D_i + M * P_IJ;
            D_i_1 = D_i_1 - M * P_IJ;
        }
        opt.add(sum_i == 1);
        if (i > 0) {
            OperatorNodePtr prevNode = operatorPath[i - 1]->as<OperatorNode>();
            std::any prop = prevNode->getProperty("output");
            double output = std::any_cast<double>(prop);
            std::string prevID = std::to_string(prevNode->getId());
            distances.insert(std::make_pair(prevID, D_i_1));
            opt.add(D_i_1 >= 0);
            cost = cost + output * D_i_1;
        }
        D_i_1 = D_i;
        D_i = c.int_val(0);
    }
    opt.minimize(cost);
    if (sat == opt.check()) {
        model m = opt.get_model();
        NES_INFO("ILPStrategy:\n" << m);
    } else {
        NES_INFO("ILPStrategy: Solver failed.");
    }

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