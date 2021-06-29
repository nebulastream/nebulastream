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
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
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

void ILPStrategy::computeDistanceRecursive(TopologyNodePtr node, std::map<std::string, double>& mileageMap){
    std::string topologyID = std::to_string(node->getId());
    auto& parents = node->getParents();
    if (parents.empty()){
        mileageMap[topologyID] = 0.0;
        return;
    }
    TopologyNodePtr parent = parents[0]->as<TopologyNode>();
    std::string parentID = std::to_string(parent->getId());
    if(mileageMap.find(parentID) == mileageMap.end()){
        computeDistanceRecursive(parent, mileageMap);
    }
    mileageMap[topologyID] = 1.0 / node->getLinkProperty(parent)->bandwidth + mileageMap[parentID];
}

std::map<std::string, double> ILPStrategy::computeDistanceHeuristic(QueryPlanPtr queryPlan){
    std::map<std::string, double> mileageMap; // (operatorid, M)
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

    for (const auto& sourceNode : sourceOperators) {
        std::string streamName = sourceNode->getSourceDescriptor()->getStreamName();
        for (const auto& sourceToplogyNode : streamCatalog->getSourceNodesForLogicalStream(streamName)) {
            computeDistanceRecursive(sourceToplogyNode, mileageMap);
        }
    }

    return mileageMap;
}

void ILPStrategy::assignOperatorPropertiesRecursive(LogicalOperatorNodePtr operatorNode, double input){
    int cost = 1;
    double dmf = 1;

    NodePtr nodePtr = operatorNode->as<Node>();
    if(operatorNode->instanceOf<SinkLogicalOperatorNode>()){
        dmf = 0;
        cost = 0;
    } else if(operatorNode->instanceOf<FilterLogicalOperatorNode>()){
        dmf = 0.5;
        cost = 1;
    } else if(operatorNode->instanceOf<MapLogicalOperatorNode>()){
        dmf = 2;
        cost = 2;
    } else if(operatorNode->instanceOf<JoinLogicalOperatorNode>()){
        cost = 2;
    } else if(operatorNode->instanceOf<UnionLogicalOperatorNode>()){
        cost = 2;
    } else if(operatorNode->instanceOf<ProjectionLogicalOperatorNode>()){
        cost = 1;
    }else if(operatorNode->instanceOf<SourceLogicalOperatorNode>()){
        cost = 0;
    }

    double output = input * dmf;
    operatorNode->addProperty("output", output);
    operatorNode->addProperty("cost", cost);

    for(const auto& parent : operatorNode->getParents()) {
        assignOperatorPropertiesRecursive(parent->as<LogicalOperatorNode>(), output);
    }
}

void ILPStrategy::applyOperatorHeuristics(QueryPlanPtr queryPlan){
    std::map<std::string, double> mileageMap; // (operatorid, M)
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

    for (const auto& sourceNode : sourceOperators) {
        double datarate = 100; // ingestion rate * tuplesize; TODO use actual values
        int operatorcost = 0;
        sourceNode->addProperty("output", datarate);
        sourceNode->addProperty("cost", operatorcost);
        for(const auto& parent : sourceNode->getParents()) {
            assignOperatorPropertiesRecursive(parent->as<LogicalOperatorNode>(), datarate);
        }
    }
}

void ILPStrategy::addPath(context& c,
                          optimize& opt,
                          std::vector<NodePtr>& operatorPath,
                          std::vector<NodePtr>& topologyPath,
                          std::map<std::string, OperatorNodePtr>& operatorNodes,
                          std::map<std::string, TopologyNodePtr>& topologyNodes,
                          std::map<std::string, expr>& placementVariables,
                          std::map<std::string, expr>& positions,
                          std::map<std::string, expr>& utilizations,
                          std::map<std::string, double>& mileages) {

    for (int i = 0; i < operatorPath.size(); i++) {
        OperatorNodePtr operatorNode = operatorPath[i]->as<OperatorNode>();
        std::string operatorID = std::to_string(operatorNode->getId());

        if(operatorNodes.find(operatorID) != operatorNodes.end()) {
            expr path_constraint = c.int_val(0);
            for (int j = 0; j < topologyPath.size(); j++) {
                TopologyNodePtr topologyNode = topologyPath[j]->as<TopologyNode>();
                std::string topologyID = std::to_string(topologyNode->getId());
                topologyNodes[topologyID] = topologyNode;

                std::string variableID = operatorID + "," + topologyID;
                auto iter = placementVariables.find(variableID);
                if(iter != placementVariables.end()) {
                    path_constraint = path_constraint + iter->second;
                }
            }
            opt.add(path_constraint == 1);
            break; // all following nodes already created
        }

        operatorNodes[operatorID] = operatorNode;
        expr sum_i = c.int_val(0);
        expr D_i = c.int_val(0);
        for (int j = 0; j < topologyPath.size(); j++) {
            TopologyNodePtr topologyNode = topologyPath[j]->as<TopologyNode>();
            std::string topologyID = std::to_string(topologyNode->getId());
            topologyNodes[topologyID] = topologyNode;

            // create placement variable and constraint to {0,1}
            std::string variableID = operatorID + "," + topologyID;
            expr P_IJ = c.int_const(variableID.c_str());
            if (i == 0 && j == 0 || i == operatorPath.size() - 1 && j == topologyPath.size() - 1) {
                opt.add(P_IJ == 1); // fix sources and sinks
            } else {
                opt.add(P_IJ == 0 || P_IJ == 1);
            }
            placementVariables.insert(std::make_pair(variableID, P_IJ));
            sum_i = sum_i + P_IJ;

            // add to node utilization
            std::any prop = operatorNode->getProperty("slots");
            auto slots = std::any_cast<int>(prop);
            auto iterator = utilizations.find(topologyID);
            if(iterator != utilizations.end()){
                iterator->second = iterator->second + slots * P_IJ;
            } else {
                utilizations.insert(std::make_pair(topologyID,slots * P_IJ));
            }

            // add distance to root (positive part of distance equation)
            double M = mileages[topologyID];
            D_i = D_i + c.real_val(std::to_string(M).c_str()) * P_IJ;
        }
        positions.insert(std::make_pair(operatorID,D_i));
        // add constraint that operator is placed exactly once on topology path
        opt.add(sum_i == 1);
    }
}

bool ILPStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan){
    //const QueryId queryId = queryPlan->getQueryId();

    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

    context c;
    optimize opt(c);

    std::map<std::string, OperatorNodePtr> operatorNodes;   // operatorID
    std::map<std::string, TopologyNodePtr> topologyNodes;   // topologyID
    std::map<std::string, expr> placementVariables;         // (operatorID,topologyID)
    std::map<std::string, expr> positions;                  // operatorID
    std::map<std::string, expr> utilizations;               // topologyID
    std::map<std::string, double> milages = computeDistanceHeuristic(queryPlan);
    applyOperatorHeuristics(queryPlan);

    for (const auto& sourceNode : sourceOperators) {
        std::string streamName = sourceNode->getSourceDescriptor()->getStreamName();
        TopologyNodePtr sourceToplogyNode = streamCatalog->getSourceNodesForLogicalStream(streamName)[0];
        std::vector<NodePtr> operatorPath = findPathToRoot(sourceNode);
        std::vector<NodePtr> topologyPath = findPathToRoot(sourceToplogyNode);
        addPath(c, opt, operatorPath, topologyPath, operatorNodes, topologyNodes, placementVariables, positions, utilizations, milages);
    }

    // calculate network cost
    expr cost_net = c.int_val(0);
    for (auto const& [operatorID, position] : positions) {
        OperatorNodePtr operatorNode = operatorNodes[operatorID]->as<OperatorNode>();
        if(operatorNode->getParents().empty())
            continue;
        OperatorNodePtr operatorParent = operatorNode->getParents()[0]->as<OperatorNode>();
        std::string operatorParentID = std::to_string(operatorParent->getId());
        expr distance = position - positions.find(operatorParentID)->second;
        std::cout << "distance: " << operatorID << " " << distance << std::endl;
        std::any prop = operatorNode->getProperty("output");
        auto output = std::any_cast<double>(prop);
        cost_net = cost_net + c.real_val(std::to_string(output).c_str()) * distance;
    }
    std::cout << "cost_net: " << cost_net << std::endl;

    // calculate overutilization cost
    expr cost_ou = c.int_val(0);
    for (auto const& [topologyID, utilization] : utilizations) {
        std::string slackID = "S" + topologyID;
        expr S_j = c.int_const(slackID.c_str());
        TopologyNodePtr topologyNode = topologyNodes[topologyID]->as<TopologyNode>();
        std::any prop = topologyNode->getNodeProperty("slots");
        auto output = std::any_cast<int>(prop);
        opt.add(S_j >= 0);
        opt.add(utilization - S_j<= output);
        cost_ou = cost_ou + S_j;
    }

    // optimize ILP problem and print solution
    opt.minimize(1 * cost_net + 1 * cost_ou);
    if (sat == opt.check()) {
        model m = opt.get_model();
        NES_INFO("ILPStrategy:\n" << m);

        std::map<OperatorNodePtr, TopologyNodePtr> operatorToTopologyNodeMap;
        for (auto const& [topologyID, P] : placementVariables) {
            if (m.eval(P).get_numeral_int() == 1) {
                int operatorId = std::stoi(topologyID.substr(0, topologyID.find(",")));
                int topologyNodeId = std::stoi(topologyID.substr(topologyID.find(",") + 1));
                OperatorNodePtr operatorNode = queryPlan->getOperatorWithId(operatorId);
                TopologyNodePtr topologyNode = topology->findNodeWithId(topologyNodeId);
                operatorToTopologyNodeMap.insert(std::make_pair(operatorNode, topologyNode));
            }
        }

        NES_INFO("Solver found solution with cost: " << m.eval(cost_net).get_decimal_string(4));
        for(auto const& [operatorNode, topologyNode] : operatorToTopologyNodeMap){
            NES_INFO("Operator " << operatorNode->toString() <<" is executed on Topology Node " << topologyNode->toString());
        }
        return true;
    } else {
        NES_INFO("ILPStrategy: Solver failed.");
        return false;
    }

    /*try {
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
    */
    return true;
}

/**
 * assigns operators to topology nodes based on ILP solution
 */
void ILPStrategy::placeOperators() {

}

}// namespace NES::Optimizer