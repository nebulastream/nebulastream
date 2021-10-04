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

#include "z3++.h"
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Optimizer/QueryPlacement/ILPStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
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

bool ILPStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan) {
    const QueryId queryId = queryPlan->getQueryId();
    NES_INFO("ILPStrategy: Performing placement of the input query plan with id " << queryId);
    NES_INFO("ILPStrategy: And query plan \n" << queryPlan->toString());

    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

    context c;
    optimize opt(c);

    std::map<std::string, OperatorNodePtr> operatorNodes;
    std::map<std::string, TopologyNodePtr> topologyNodes;
    std::map<std::string, expr> placementVariables;// (operatorID,topologyID)
    std::map<std::string, expr> positions;         // operatorID
    std::map<std::string, expr> utilizations;      // topologyID
    std::map<std::string, double> milages = computeDistanceHeuristic(queryPlan);

    for (const auto& sourceNode : sourceOperators) {
        std::string streamName = sourceNode->getSourceDescriptor()->getStreamName();
        TopologyNodePtr sourceToplogyNode = streamCatalog->getSourceNodesForLogicalStream(streamName)[0];
        std::vector<NodePtr> operatorPath = findPathToRoot(sourceNode);
        std::vector<NodePtr> topologyPath = findPathToRoot(sourceToplogyNode);
        auto success = addPath(c,
                               opt,
                               operatorPath,
                               topologyPath,
                               operatorNodes,
                               topologyNodes,
                               placementVariables,
                               positions,
                               utilizations,
                               milages);
        if (!success) {
            NES_ERROR("ILPStrategy: an error occurred when adding path.");
        }
    }

    // calculate network cost = sum over all operators (output of operator * distance of operator)
    expr cost_net = c.int_val(0);
    for (auto const& [operatorID, position] : positions) {
        OperatorNodePtr operatorNode = operatorNodes[operatorID]->as<OperatorNode>();
        if (operatorNode->getParents().empty())
            continue;
        OperatorNodePtr operatorParent = operatorNode->getParents()[0]->as<OperatorNode>();
        std::string operatorParentID = std::to_string(operatorParent->getId());

        expr distance = position - positions.find(operatorParentID)->second;
        NES_DEBUG("distance: " << operatorID << " " << distance << std::endl);

        std::any prop = operatorNode->getProperty("output");
        auto output = std::any_cast<double>(prop);

        cost_net = cost_net + c.real_val(std::to_string(output).c_str()) * distance;
    }
    NES_DEBUG("cost_net: " << cost_net << std::endl);

    // calculate over-utilization cost = sum over all topology nodes (slack)
    // slack = utilization of node - slack <= available slots on node
    expr cost_ou = c.int_val(0);
    for (auto const& [topologyID, utilization] : utilizations) {
        std::string slackID = "S" + topologyID;
        expr S_j = c.int_const(slackID.c_str());

        TopologyNodePtr topologyNode = topologyNodes[topologyID]->as<TopologyNode>();
        std::any prop = topologyNode->getNodeProperty("slots");
        auto output = std::any_cast<int>(prop);

        opt.add(S_j >= 0);
        opt.add(utilization - S_j <= output);

        cost_ou = cost_ou + S_j;
    }

    expr weightOU = c.real_val(std::to_string(this->overUtilizationCostWeight).c_str());
    expr weightNet = c.real_val(std::to_string(this->networkCostWeight).c_str());

    // optimize ILP problem and print solution
    opt.minimize(weightNet * cost_net + weightOU * cost_ou);
    if (sat != opt.check()) {
        NES_ERROR("ILPStrategy: Solver failed.");
        return false;
    }

    model z3Model = opt.get_model();
    NES_DEBUG("ILPStrategy:model: \n" << z3Model);

    std::map<OperatorNodePtr, TopologyNodePtr> operatorToTopologyNodeMap;
    for (auto const& [topologyID, P] : placementVariables) {
        if (z3Model.eval(P).get_numeral_int() == 1) {
            int operatorId = std::stoi(topologyID.substr(0, topologyID.find(",")));
            int topologyNodeId = std::stoi(topologyID.substr(topologyID.find(",") + 1));
            OperatorNodePtr operatorNode = queryPlan->getOperatorWithId(operatorId);
            TopologyNodePtr topologyNode = topology->findNodeWithId(topologyNodeId);
            operatorToTopologyNodeMap.insert(std::make_pair(operatorNode, topologyNode));
        }
    }

    NES_INFO("Solver found solution with cost: " << z3Model.eval(cost_net).get_decimal_string(4));
    for (auto const& [operatorNode, topologyNode] : operatorToTopologyNodeMap) {
        NES_INFO("Operator " << operatorNode->toString() << " is executed on Topology Node " << topologyNode->toString());
    }

    placeOperators(queryPlan, z3Model, placementVariables);

    return runTypeInferencePhase(queryPlan->getQueryId());
}

std::vector<NodePtr> ILPStrategy::findPathToRoot(NodePtr sourceNode) {
    std::vector<NodePtr> path;
    path.push_back(sourceNode);
    while (!path.back()->getParents().empty()) {
        path.push_back(path.back()->getParents()[0]);// assuming single parent.
    }
    return path;
}

void ILPStrategy::computeDistanceRecursive(TopologyNodePtr node, std::map<std::string, double>& mileages) {
    std::string topologyID = std::to_string(node->getId());
    auto& parents = node->getParents();
    if (parents.empty()) {
        mileages[topologyID] = 0.0;
        return;
    }
    TopologyNodePtr parent = parents[0]->as<TopologyNode>();
    std::string parentID = std::to_string(parent->getId());
    if (mileages.find(parentID) == mileages.end()) {
        computeDistanceRecursive(parent, mileages);
    }
    mileages[topologyID] = 1.0 / node->getLinkProperty(parent)->bandwidth + mileages[parentID];
}

std::map<std::string, double> ILPStrategy::computeDistanceHeuristic(QueryPlanPtr queryPlan) {
    std::map<std::string, double> mileageMap;// (operatorid, M)
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

    for (const auto& sourceNode : sourceOperators) {
        std::string streamName = sourceNode->getSourceDescriptor()->getStreamName();
        for (const auto& sourceToplogyNode : streamCatalog->getSourceNodesForLogicalStream(streamName)) {
            computeDistanceRecursive(sourceToplogyNode, mileageMap);
        }
    }

    return mileageMap;
}

bool ILPStrategy::addPath(context& context,
                          optimize& opt,
                          std::vector<NodePtr>& operatorNodePath,
                          std::vector<NodePtr>& topologyNodePath,
                          std::map<std::string, OperatorNodePtr>& operatorIdToNodeMap,
                          std::map<std::string, TopologyNodePtr>& topologyNodeIdToNodeMap,
                          std::map<std::string, expr>& placementVariables,
                          std::map<std::string, expr>& distances,
                          std::map<std::string, expr>& utilizations,
                          std::map<std::string, double>& mileages) {

    for (uint64_t i = 0; i < operatorNodePath.size(); i++) {
        OperatorNodePtr operatorNode = operatorNodePath[i]->as<OperatorNode>();
        std::string operatorID = std::to_string(operatorNode->getId());

        if (operatorIdToNodeMap.find(operatorID) != operatorIdToNodeMap.end()) {
            expr path_constraint = context.int_val(0);
            for (uint64_t j = 0; j < topologyNodePath.size(); j++) {
                TopologyNodePtr topologyNode = topologyNodePath[j]->as<TopologyNode>();
                std::string topologyID = std::to_string(topologyNode->getId());
                topologyNodeIdToNodeMap[topologyID] = topologyNode;

                std::string variableID = operatorID + "," + topologyID;
                auto iter = placementVariables.find(variableID);
                if (iter != placementVariables.end()) {
                    path_constraint = path_constraint + iter->second;
                }
            }
            opt.add(path_constraint == 1);
            break;// all following nodes already created
        }

        operatorIdToNodeMap[operatorID] = operatorNode;
        expr sum_i = context.int_val(0);
        expr D_i = context.int_val(0);
        for (uint64_t j = 0; j < topologyNodePath.size(); j++) {
            TopologyNodePtr topologyNode = topologyNodePath[j]->as<TopologyNode>();
            std::string topologyID = std::to_string(topologyNode->getId());
            topologyNodeIdToNodeMap[topologyID] = topologyNode;

            // create placement variable and constraint to {0,1}
            std::string variableID = operatorID + "," + topologyID;
            expr P_IJ = context.int_const(variableID.c_str());
            if ((i == 0 && j == 0) || (i == operatorNodePath.size() - 1 && j == topologyNodePath.size() - 1)) {
                opt.add(P_IJ == 1);// fix sources and sinks
            } else {
                opt.add(P_IJ == 0 || P_IJ == 1);
            }
            placementVariables.insert(std::make_pair(variableID, P_IJ));
            sum_i = sum_i + P_IJ;

            // add to node utilization
            std::any prop = operatorNode->getProperty("cost");
            auto slots = std::any_cast<int>(prop);
            auto iterator = utilizations.find(topologyID);
            if (iterator != utilizations.end()) {
                iterator->second = iterator->second + slots * P_IJ;
            } else {
                // utilization of a node = slots (i.e. computing cost of operator) * placement variable
                utilizations.insert(std::make_pair(topologyID, slots * P_IJ));
            }

            // add distance to root (positive part of distance equation)
            double M = mileages[topologyID];
            D_i = D_i + context.real_val(std::to_string(M).c_str()) * P_IJ;
        }
        distances.insert(std::make_pair(operatorID, D_i));
        // add constraint that operator is placed exactly once on topology path
        opt.add(sum_i == 1);
    }
    return true;
}

bool ILPStrategy::placeOperators(QueryPlanPtr queryPlan, model& model, std::map<std::string, expr>& placementVariables) {
    auto topologyIterator = NES::DepthFirstNodeIterator(topology->getRoot()).begin();
    std::vector<std::vector<bool>> binaryMapping;

    while (topologyIterator != DepthFirstNodeIterator::end()) {
        // get the ExecutionNode for the current topology Node
        auto currentTopologyNode = (*topologyIterator)->as<TopologyNode>();
        std::string topologyID = std::to_string(currentTopologyNode->getId());

        std::vector<bool> tmp;

        // iterate to all operator in current query plan
        auto queryPlanIterator = NES::QueryPlanIterator(queryPlan);
        for (auto&& op : queryPlanIterator) {
            OperatorNodePtr operatorNode = op->as<OperatorNode>();
            std::string operatorID = std::to_string(operatorNode->getId());
            std::string variableID = operatorID + "," + topologyID;
            auto iter = placementVariables.find(variableID);
            if (iter != placementVariables.end()) {
                tmp.push_back(model.eval(iter->second).get_numeral_int() == 1);
            } else {
                tmp.push_back(false);
            }
        }
        binaryMapping.push_back(tmp);
        ++topologyIterator;
    }

    // apply the placement from the specified binary mapping
    auto assignSuccess = assignMappingToTopology(topology, queryPlan, binaryMapping);
    if (!assignSuccess) {
        return false;
    }
    addNetworkSourceAndSinkOperators(queryPlan);

    return true;
}

double ILPStrategy::getOverUtilizationCostWeight() { return this->overUtilizationCostWeight; }

double ILPStrategy::getNetworkCostWeight() { return this->networkCostWeight; }

void ILPStrategy::setOverUtilizationWeight(double weight) { this->overUtilizationCostWeight = weight; }

void ILPStrategy::setNetworkCostWeight(double weight) { this->networkCostWeight = weight; }

}// namespace NES::Optimizer