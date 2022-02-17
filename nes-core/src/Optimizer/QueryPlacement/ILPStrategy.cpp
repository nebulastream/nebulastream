/*
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

#include <Catalogs/Source/SourceCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
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
#include <z3++.h>

namespace NES::Optimizer {

std::unique_ptr<BasePlacementStrategy> ILPStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                           TopologyPtr topology,
                                                           TypeInferencePhasePtr typeInferencePhase,
                                                           z3::ContextPtr z3Context) {
    return std::make_unique<ILPStrategy>(ILPStrategy(globalExecutionPlan, topology, typeInferencePhase, z3Context));
}

ILPStrategy::ILPStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                         TopologyPtr topology,
                         TypeInferencePhasePtr typeInferencePhase,
                         z3::ContextPtr z3Context)
    : BasePlacementStrategy(globalExecutionPlan, topology, typeInferencePhase), z3Context(std::move(z3Context)) {}

bool ILPStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan) {
    const QueryId queryId = queryPlan->getQueryId();
    NES_INFO("ILPStrategy: Performing placement of the input query plan with id " << queryId);
    NES_INFO("ILPStrategy: And query plan \n" << queryPlan->toString());

    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

    z3::optimize opt(*z3Context);

    std::map<std::string, OperatorNodePtr> operatorNodes;
    std::map<std::string, TopologyNodePtr> topologyNodes;
    std::map<std::string, z3::expr> placementVariables;
    std::map<std::string, z3::expr> distances;
    std::map<std::string, z3::expr> utilizations;
    std::map<std::string, double> milages = computeDistanceHeuristic(queryPlan);

    // 1. Construct the placementVariable, compute distance, utilization and mileages
    for (const auto& sourceNode : sourceOperators) {
        std::string streamName = sourceNode->getSourceDescriptor()->getLogicalSourceName();
        //FIXME: #2485 Dwi: Can we extract this information from the operators?
        //TopologyNodePtr sourceToplogyNode = sourceCatalog->getSourceNodesForLogicalStream(streamName)[0];
        //std::vector<NodePtr> operatorPath = findPathToRoot(sourceNode);
        //std::vector<NodePtr> topologyPath = findPathToRoot(sourceToplogyNode);
        //auto success = addConstraints(z3Context,
        //                              opt,
        //                              operatorPath,
        //                              topologyPath,
        //                              operatorNodes,
        //                              topologyNodes,
        //                              placementVariables,
        //                              distances,
        //                              utilizations,
        //                              milages);
        if (false /*!success*/) {
            NES_ERROR("ILPStrategy: an error occurred when adding path.");
        }
    }

    // 2. Calculate the network cost.
    // Network cost = sum over all operators (output of operator * distance of operator)
    auto cost_net = z3Context->int_val(0);// initialize the network cost with 0

    for (auto const& [operatorID, position] : distances) {
        OperatorNodePtr operatorNode = operatorNodes[operatorID]->as<OperatorNode>();
        if (operatorNode->getParents().empty())
            continue;
        OperatorNodePtr operatorParent = operatorNode->getParents()[0]->as<OperatorNode>();
        std::string operatorParentID = std::to_string(operatorParent->getId());

        auto distance = position - distances.find(operatorParentID)->second;
        NES_DEBUG("distance: " << operatorID << " " << distance);

        std::any prop = operatorNode->getProperty("output");
        auto output = std::any_cast<double>(prop);

        cost_net = cost_net + z3Context->real_val(std::to_string(output).c_str()) * distance;
    }
    NES_DEBUG("cost_net: " << cost_net);

    // 3. Calculate the node over-utilization cost.
    // Over-utilization cost = sum of the over-utilization of all nodes
    auto overUtilizationCost = z3Context->int_val(0);// initialize the over-utilization cost with 0
    for (auto const& [topologyID, utilization] : utilizations) {
        std::string overUtilizationId = "S" + topologyID;
        auto currentOverUtilization = z3Context->int_const(overUtilizationId.c_str());// an integer expression of the slack

        // Obtain the available slot in the current node
        TopologyNodePtr topologyNode = topologyNodes[topologyID]->as<TopologyNode>();
        std::any prop = topologyNode->getNodeProperty("slots");
        auto availableSlot = std::any_cast<int>(prop);

        opt.add(currentOverUtilization >= 0);// we only penalize over-utilization, hence its value should be >= 0.
        opt.add(utilization - currentOverUtilization <= availableSlot);// formula for the over-utilization

        overUtilizationCost = overUtilizationCost + currentOverUtilization;
    }

    auto weightOverUtilization = z3Context->real_val(std::to_string(this->overUtilizationCostWeight).c_str());
    auto weightNetwork = z3Context->real_val(std::to_string(this->networkCostWeight).c_str());

    // 4. Optimize ILP problem and print solution.
    opt.minimize(weightNetwork * cost_net + weightOverUtilization * overUtilizationCost);// where the actual optimization happen

    // 5. Check if we have solution, return false if that is not the case
    if (z3::sat != opt.check()) {
        NES_ERROR("ILPStrategy: Solver failed.");
        return false;
    }

    // At this point, we already get the solution.
    // 6. Get the model to retrieve the optimization solution
    auto z3Model = opt.get_model();
    NES_DEBUG("ILPStrategy:model: \n" << z3Model);

    // 7. Pick the solution which has placement decision of 1, i.e., the ILP decide to place the operator in that node
    std::map<OperatorNodePtr, TopologyNodePtr> operatorToTopologyNodeMap;
    for (auto const& [topologyID, P] : placementVariables) {
        if (z3Model.eval(P).get_numeral_int() == 1) {// means we place the operator in the node
            int operatorId = std::stoi(topologyID.substr(0, topologyID.find(",")));
            int topologyNodeId = std::stoi(topologyID.substr(topologyID.find(",") + 1));
            OperatorNodePtr operatorNode = queryPlan->getOperatorWithId(operatorId);
            TopologyNodePtr topologyNode = topology->findNodeWithId(topologyNodeId);

            // collect the solution to operatorToTopologyNodeMap
            operatorToTopologyNodeMap.insert(std::make_pair(operatorNode, topologyNode));
        }
    }

    NES_INFO("Solver found solution with cost: " << z3Model.eval(cost_net).get_decimal_string(4));
    for (auto const& [operatorNode, topologyNode] : operatorToTopologyNodeMap) {
        NES_INFO("Operator " << operatorNode->toString() << " is executed on Topology Node " << topologyNode->toString());
    }

    // 8. Apply the operator placement to the execution nodes based on the ILP solution
    placeOperators(queryPlan, z3Model, placementVariables);

    // 9. Run the type inference phase and return the status
    return runTypeInferencePhase(queryPlan->getQueryId(), queryPlan->getFaultToleranceType(), queryPlan->getLineageType());
}

std::vector<NodePtr> ILPStrategy::findPathToRoot(NodePtr sourceNode) {
    std::vector<NodePtr> path;
    path.push_back(sourceNode);
    while (!path.back()->getParents().empty()) {
        // FIXME: Assuming a tree structure, hence a node can only has a single parent (issue #2290)
        path.push_back(path.back()->getParents()[0]);
    }
    return path;
}

void ILPStrategy::computeDistanceRecursive(TopologyNodePtr node, std::map<std::string, double>& mileages) {
    std::string topologyID = std::to_string(node->getId());
    auto& parents = node->getParents();
    // if the current node has no parent (i.e., is a root node), then the mileages is 0
    if (parents.empty()) {
        mileages[topologyID] = 0.0;
        return;
    }

    // if the current node is not a root node, recursively compute the mileage of its first parent
    // assuming a node only has a single parent
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

    // populate the whole distance map
    for (const auto& sourceNode : sourceOperators) {
        std::string streamName = sourceNode->getSourceDescriptor()->getLogicalSourceName();
        auto nodeId = std::any_cast<uint64_t>(sourceNode->getProperty(PINNED_NODE_ID));
        auto topologyNode = topology->findNodeWithId(nodeId);
        //FIXME: #2485 Dwi: can we use the pinning information from the source operators?
        // for (const auto& sourceToplogyNode : sourceCatalog->getSourceNodesForLogicalStream(streamName)) {
        //    computeDistanceRecursive(sourceToplogyNode, mileageMap);
        // }
    }

    return mileageMap;
}

bool ILPStrategy::addConstraints(z3::ContextPtr z3Context,
                                 z3::optimize& opt,
                                 std::vector<NodePtr>& operatorNodePath,
                                 std::vector<NodePtr>& topologyNodePath,
                                 std::map<std::string, OperatorNodePtr>& operatorIdToNodeMap,
                                 std::map<std::string, TopologyNodePtr>& topologyNodeIdToNodeMap,
                                 std::map<std::string, z3::expr>& placementVariable,
                                 std::map<std::string, z3::expr>& operatorIdDistancesMap,
                                 std::map<std::string, z3::expr>& operatorIdUtilizationsMap,
                                 std::map<std::string, double>& mileages) {

    for (uint64_t i = 0; i < operatorNodePath.size(); i++) {
        OperatorNodePtr operatorNode = operatorNodePath[i]->as<OperatorNode>();
        std::string operatorID = std::to_string(operatorNode->getId());

        if (operatorIdToNodeMap.find(operatorID) != operatorIdToNodeMap.end()) {
            // Initialize the path constraint variable to 0
            auto pathConstraint = z3Context->int_val(0);
            for (uint64_t j = 0; j < topologyNodePath.size(); j++) {
                TopologyNodePtr topologyNode = topologyNodePath[j]->as<TopologyNode>();
                std::string topologyID = std::to_string(topologyNode->getId());
                topologyNodeIdToNodeMap[topologyID] = topologyNode;

                std::string variableID = operatorID + "," + topologyID;
                auto iter = placementVariable.find(variableID);
                if (iter != placementVariable.end()) {
                    pathConstraint = pathConstraint + iter->second;
                }
            }
            opt.add(pathConstraint == 1);
            break;// all following nodes already created
        }

        operatorIdToNodeMap[operatorID] = operatorNode;
        // Fill the placement variable, utilization, and distance map
        auto sum_i = z3Context->int_val(0);
        auto D_i = z3Context->int_val(0);
        for (uint64_t j = 0; j < topologyNodePath.size(); j++) {
            TopologyNodePtr topologyNode = topologyNodePath[j]->as<TopologyNode>();
            std::string topologyID = std::to_string(topologyNode->getId());
            topologyNodeIdToNodeMap[topologyID] = topologyNode;

            // create placement variable and constraint to {0,1}
            std::string variableID = operatorID + "," + topologyID;
            auto P_IJ = z3Context->int_const(variableID.c_str());
            if ((i == 0 && j == 0) || (i == operatorNodePath.size() - 1 && j == topologyNodePath.size() - 1)) {
                opt.add(P_IJ == 1);// Fix the placement of source and sink
            } else {
                // The binary decision on whether to place or not, hence we constrain it to be either 0 or 1
                opt.add(P_IJ == 0 || P_IJ == 1);
            }
            placementVariable.insert(std::make_pair(variableID, P_IJ));
            sum_i = sum_i + P_IJ;

            // add to node utilization
            std::any prop = operatorNode->getProperty("cost");
            auto slots = std::any_cast<int>(prop);
            auto iterator = operatorIdUtilizationsMap.find(topologyID);
            if (iterator != operatorIdUtilizationsMap.end()) {
                iterator->second = iterator->second + slots * P_IJ;
            } else {
                // utilization of a node = slots (i.e. computing cost of operator) * placement variable
                operatorIdUtilizationsMap.insert(std::make_pair(topologyID, slots * P_IJ));
            }

            // add distance to root (positive part of distance equation)
            double M = mileages[topologyID];
            D_i = D_i + z3Context->real_val(std::to_string(M).c_str()) * P_IJ;
        }
        operatorIdDistancesMap.insert(std::make_pair(operatorID, D_i));
        // add constraint that operator is placed exactly once on topology path
        opt.add(sum_i == 1);
    }
    return true;
}

bool ILPStrategy::placeOperators(QueryPlanPtr queryPlan,
                                 z3::model& z3Model,
                                 std::map<std::string, z3::expr>& placementVariables) {
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
                tmp.push_back(z3Model.eval(iter->second).get_numeral_int() == 1);
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
    //addNetworkSourceAndSinkOperators(queryPlan);
    return true;
}

double ILPStrategy::getOverUtilizationCostWeight() { return this->overUtilizationCostWeight; }

double ILPStrategy::getNetworkCostWeight() { return this->networkCostWeight; }

void ILPStrategy::setOverUtilizationWeight(double weight) { this->overUtilizationCostWeight = weight; }

void ILPStrategy::setNetworkCostWeight(double weight) { this->networkCostWeight = weight; }

bool ILPStrategy::updateGlobalExecutionPlan(QueryId /*queryId*/,
                                            FaultToleranceType /*faultToleranceType*/,
                                            LineageType /*lineageType*/,
                                            const std::vector<OperatorNodePtr>& /*pinnedUpStreamNodes*/,
                                            const std::vector<OperatorNodePtr>& /*pinnedDownStreamNodes*/) {
    NES_NOT_IMPLEMENTED();
}

}// namespace NES::Optimizer