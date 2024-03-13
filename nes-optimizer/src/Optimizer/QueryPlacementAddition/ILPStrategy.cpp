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
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Optimizer/Exceptions/QueryPlacementAdditionException.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacementAddition/BottomUpStrategy.hpp>
#include <Optimizer/QueryPlacementAddition/ILPStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <z3++.h>

namespace NES::Optimizer {

BasePlacementStrategyPtr ILPStrategy::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                             const TopologyPtr& topology,
                                             const TypeInferencePhasePtr& typeInferencePhase,
                                             PlacementAmendmentMode placementAmendmentMode) {
    z3::config cfg;
    cfg.set("timeout", 1000);
    cfg.set("model", false);
    cfg.set("type_check", false);
    const auto& z3Context = std::make_shared<z3::context>(cfg);
    return std::make_unique<ILPStrategy>(
        ILPStrategy(globalExecutionPlan, topology, typeInferencePhase, z3Context, placementAmendmentMode));
}

ILPStrategy::ILPStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                         const TopologyPtr& topology,
                         const TypeInferencePhasePtr& typeInferencePhase,
                         const z3::ContextPtr& z3Context,
                         PlacementAmendmentMode placementAmendmentMode)
    : BasePlacementAdditionStrategy(globalExecutionPlan, topology, typeInferencePhase, placementAmendmentMode),
      z3Context(z3Context) {}

PlacementAdditionResult ILPStrategy::updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                                               const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                                               const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                                               DecomposedQueryPlanVersion querySubPlanVersion) {

    try {
        NES_INFO("Performing placement of the input query plan with id {}", sharedQueryId);

        // 1. Create copy of the query plan
        auto copy =
            CopiedPinnedOperators::create(pinnedUpStreamOperators, pinnedDownStreamOperators, operatorIdToOriginalOperatorMap);

        // 2. Find the path where operators need to be placed
        performPathSelection(copy.copiedPinnedUpStreamOperators, copy.copiedPinnedDownStreamOperators);

        z3::optimize opt(*z3Context);
        std::map<std::string, z3::expr> placementVariables;
        std::map<OperatorId, z3::expr> operatorPositionMap;
        std::map<uint64_t, z3::expr> nodeUtilizationMap;
        std::map<uint64_t, double> nodeMileageMap = computeMileage(copy.copiedPinnedUpStreamOperators);

        // 2. Construct the placementVariable, compute distance, utilization and mileages
        for (const auto& pinnedUpStreamOperator : copy.copiedPinnedUpStreamOperators) {

            //2.1 Find all path between pinned upstream and downstream operators
            std::vector<NodePtr> operatorPath;
            operatorPath.push_back(pinnedUpStreamOperator);
            while (!operatorPath.back()->getParents().empty()) {

                //Before further processing please identify if the operator to be processed is among the collection of pinned downstream operators
                auto& operatorToProcess = operatorPath.back();
                auto isPinnedDownStreamOperator = std::find_if(
                    copy.copiedPinnedDownStreamOperators.begin(),
                    copy.copiedPinnedDownStreamOperators.end(),
                    [operatorToProcess](const LogicalOperatorPtr& pinnedDownStreamOperator) {
                        return pinnedDownStreamOperator->getId() == operatorToProcess->as_if<LogicalOperator>()->getId();
                    });

                //Skip further processing if encountered pinned downstream operator
                if (isPinnedDownStreamOperator != copy.copiedPinnedDownStreamOperators.end()) {
                    NES_DEBUG("Found pinned downstream operator. Skipping further downstream operators.");
                    break;
                }

                //Look for the next downstream operators and add them to the path.
                auto downstreamOperators = operatorToProcess->getParents();

                if (downstreamOperators.empty()) {
                    NES_ERROR("Unable to find pinned downstream operator.");
                    return {false, {}};
                }

                uint16_t unplacedDownStreamOperatorCount = 0;
                for (auto& downstreamOperator : downstreamOperators) {

                    // FIXME: (issue #2290) Assuming a tree structure, hence a node can only have a single parent. However, a query can have
                    //  multiple sinks or parents.
                    if (unplacedDownStreamOperatorCount > 1) {
                        NES_ERROR("Current implementation can not place plan with multiple downstream operators.");
                        return {false, {}};
                    }

                    // Only include unplaced operators in the path
                    if (downstreamOperator->as_if<LogicalOperator>()->getOperatorState() != OperatorState::PLACED) {
                        operatorPath.push_back(downstreamOperator);
                        unplacedDownStreamOperatorCount++;
                    }
                }
            }

            //2.2 Find path between pinned upstream and downstream topology node
            auto upstreamPinnedNodeId = std::any_cast<WorkerId>(pinnedUpStreamOperator->getProperty(PINNED_WORKER_ID));

            auto downstreamPinnedNodeId =
                std::any_cast<WorkerId>(operatorPath.back()->as_if<LogicalOperator>()->getProperty(PINNED_WORKER_ID));

            std::vector<TopologyNodePtr> topologyPath =
                topology->findPathBetween({upstreamPinnedNodeId}, {downstreamPinnedNodeId});

            while (!topologyPath.back()->getParents().empty()) {
                //FIXME #2290: path with multiple parents not supported
                if (topologyPath[0]->getParents().size() > 1) {
                    NES_ERROR("Current implementation can not place operators on topology with multiple paths.");
                    return {false, {}};
                }
                topologyPath.emplace_back(topologyPath.back()->getParents()[0]->as<TopologyNode>());
            }

            //2.3 Add constraints to Z3 solver and compute operator distance, node utilization, and node mileage map
            addConstraints(opt,
                           operatorPath,
                           topologyPath,
                           placementVariables,
                           operatorPositionMap,
                           nodeUtilizationMap,
                           nodeMileageMap);
        }

        // 3. Calculate the network cost. (Network cost = sum over all operators (output of operator * distance of operator))
        auto cost_net = z3Context->int_val(0);// initialize the network cost with 0

        for (auto const& [operatorID, position] : operatorPositionMap) {
            LogicalOperatorPtr logicalOperator = operatorMap[operatorID]->as<LogicalOperator>();
            if (logicalOperator->getParents().empty()) {
                continue;
            }

            //Loop over downstream operators and compute network cost
            for (const auto& downStreamOperator : logicalOperator->getParents()) {
                OperatorId downStreamOperatorId = downStreamOperator->as_if<LogicalOperator>()->getId();
                //Only consider nodes that are to be placed
                if (operatorMap.find(downStreamOperatorId) != operatorMap.end()) {

                    auto distance = position - operatorPositionMap.find(downStreamOperatorId)->second;
                    NES_DEBUG("distance: {} {}", operatorID, distance.to_string());
                    double output;
                    if (!logicalOperator->hasProperty("output")) {
                        output = getDefaultOperatorOutput(logicalOperator);
                    } else {
                        std::any prop = logicalOperator->getProperty("output");
                        output = std::any_cast<double>(prop);
                    }
                    cost_net = cost_net + z3Context->real_val(std::to_string(output).c_str()) * distance;
                }
            }
        }
        NES_DEBUG("cost_net: {}", cost_net.to_string());

        // 4. Calculate the node over-utilization cost.
        // Over-utilization cost = sum of the over-utilization of all nodes
        auto overUtilizationCost = z3Context->int_val(0);// initialize the over-utilization cost with 0
        for (auto const& [topologyID, utilization] : nodeUtilizationMap) {
            std::string overUtilizationId = "S" + std::to_string(topologyID);
            auto currentOverUtilization = z3Context->int_const(overUtilizationId.c_str());// an integer expression of the slack

            // Obtain the available slot in the current node
            TopologyNodePtr topologyNode = workerIdToTopologyNodeMap[topologyID]->as<TopologyNode>();
            std::any prop = topologyNode->getNodeProperty("slots");
            auto availableSlot = std::any_cast<int>(prop);

            opt.add(currentOverUtilization >= 0);// we only penalize over-utilization, hence its value should be >= 0.
            opt.add(utilization - currentOverUtilization <= availableSlot);// formula for the over-utilization

            overUtilizationCost = overUtilizationCost + currentOverUtilization;
        }

        auto weightOverUtilization = z3Context->real_val(std::to_string(this->overUtilizationCostWeight).c_str());
        auto weightNetwork = z3Context->real_val(std::to_string(this->networkCostWeight).c_str());

        // 5. Optimize ILP problem and print solution.
        opt.minimize(weightNetwork * cost_net
                     + weightOverUtilization * overUtilizationCost);// where the actual optimization happen

        // 6. Check if we have solution, return false if that is not the case
        if (z3::sat != opt.check()) {
            NES_ERROR("Solver failed.");
            return {false, {}};
        }

        // At this point, we already get the solution.
        // 7. Get the model to retrieve the optimization solution.
        auto z3Model = opt.get_model();
        NES_DEBUG("ILPStrategy:model: {}", z3Model.to_string());
        NES_INFO("Solver found solution with cost: {}", z3Model.eval(cost_net).get_decimal_string(4));

        // 7. Pick the solution which has placement decision of 1, i.e., the ILP decide to place the operator in that node
        std::map<OperatorPtr, TopologyNodePtr> operatorToTopologyNodeMap;
        for (auto const& [topologyID, P] : placementVariables) {
            if (z3Model.eval(P).get_numeral_int() == 1) {// means we place the operator in the node
                int operatorId = std::stoi(topologyID.substr(0, topologyID.find(',')));
                int topologyNodeId = std::stoi(topologyID.substr(topologyID.find(',') + 1));
                LogicalOperatorPtr logicalOperator = operatorMap[operatorId];
                TopologyNodePtr topologyNode = workerIdToTopologyNodeMap[topologyNodeId];

                // collect the solution to operatorToTopologyNodeMap
                operatorToTopologyNodeMap.insert(std::make_pair(logicalOperator, topologyNode));
            }
        }

        NES_INFO("Solver found solution with cost: {}", z3Model.eval(cost_net).get_decimal_string(4));
        for (auto const& [operatorNode, topologyNode] : operatorToTopologyNodeMap) {
            NES_INFO("Operator {} is executed on Topology Node {}", operatorNode->toString(), topologyNode->toString());
        }

        // 8. Pin the operators based on ILP solution.
        pinOperators(z3Model, placementVariables);

        // 9. Compute query sub plans
        auto computedQuerySubPlans =
            computeDecomposedQueryPlans(sharedQueryId, copy.copiedPinnedUpStreamOperators, copy.copiedPinnedDownStreamOperators);

        // 10. add network source and sink operators
        addNetworkOperators(computedQuerySubPlans);

        // 11. update execution nodes
        return updateExecutionNodes(sharedQueryId, computedQuerySubPlans, querySubPlanVersion);
    } catch (std::exception& ex) {
        throw Exceptions::QueryPlacementAdditionException(sharedQueryId, ex.what());
    }
}

std::map<uint64_t, double> ILPStrategy::computeMileage(const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators) {
    std::map<uint64_t, double> mileageMap;// (topologyId, M)
    // populate the distance map
    for (const auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        auto nodeId = std::any_cast<uint64_t>(pinnedUpStreamOperator->getProperty(PINNED_WORKER_ID));
        auto topologyNode = workerIdToTopologyNodeMap[nodeId];
        computeDistance(topologyNode, mileageMap);
    }
    return mileageMap;
}

void ILPStrategy::computeDistance(const TopologyNodePtr& node, std::map<uint64_t, double>& mileages) {
    uint64_t topologyID = node->getId();
    auto& parents = node->getParents();
    // if the current node has no parent (i.e., is a root node), then the mileages is 0
    if (parents.empty()) {
        mileages[topologyID] = 0.0;
        return;
    }

    // if the current node is not a root node, recursively compute the mileage of its first parent
    // assuming a node only has a single parent
    TopologyNodePtr parent = parents[0]->as<TopologyNode>();
    uint64_t parentID = parent->getId();
    if (mileages.find(parentID) == mileages.end()) {
        computeDistance(parent, mileages);
    }
    mileages[topologyID] = 1.0 / node->getLinkProperty(parentID)->bandwidth + mileages[parentID];
}

void ILPStrategy::addConstraints(z3::optimize& opt,
                                 std::vector<NodePtr>& operatorNodePath,
                                 std::vector<TopologyNodePtr>& topologyNodePath,
                                 std::map<std::string, z3::expr>& placementVariable,
                                 std::map<OperatorId, z3::expr>& operatorDistanceMap,
                                 std::map<uint64_t, z3::expr>& nodeUtilizationMap,
                                 std::map<uint64_t, double>& nodeMileageMap) {

    // Creating z3 expression vector to implement an additional constraint of keeping the right order of operator nodes
    std::vector<z3::expr> P_IJ_stored;
    for (uint64_t i = 0; i < operatorNodePath.size(); i++) {
        auto operatorNode = operatorNodePath[i]->as<LogicalOperator>();
        OperatorId operatorID = operatorNode->getId();
        // we only need to store the possibilities of the node before the one we look at, so we have to erase "old" data (we keep the values of the last iteration though)
        if (i > 1) {// if i == 0 noting to erase, if i == 1 we only have one "set" of values (which we still need)
            P_IJ_stored.erase(P_IJ_stored.begin(), P_IJ_stored.begin() + topologyNodePath.size());
            NES_DEBUG("Deleting old placement constraints");
        }
        if (operatorMap.find(operatorID) != operatorMap.end()) {
            // Initialize the path constraint variable to 0
            auto pathConstraint = z3Context->int_val(0);
            for (uint64_t j = 0; j < topologyNodePath.size(); j++) {
                TopologyNodePtr topologyNode = topologyNodePath[j]->as<TopologyNode>();
                uint64_t topologyID = topologyNode->getId();
                std::string variableID = std::to_string(operatorID) + KEY_SEPARATOR + std::to_string(topologyID);
                auto iter = placementVariable.find(variableID);
                if (iter != placementVariable.end()) {
                    pathConstraint = pathConstraint + iter->second;
                }
            }
            opt.add(pathConstraint == 1);
            break;// all following nodes already created
        }

        operatorMap[operatorID] = operatorNode;
        // Fill the placement variable, utilization, and distance map
        auto sum_i = z3Context->int_val(0);
        auto D_i = z3Context->int_val(0);
        for (uint64_t j = 0; j < topologyNodePath.size(); j++) {
            TopologyNodePtr topologyNode = topologyNodePath[j]->as<TopologyNode>();
            uint64_t topologyID = topologyNode->getId();

            // create placement variable and constraint to {0,1}
            std::string variableID = std::to_string(operatorID) + KEY_SEPARATOR + std::to_string(topologyID);
            auto P_IJ = z3Context->int_const(variableID.c_str());
            if ((i == 0 && j == 0) || (i == operatorNodePath.size() - 1 && j == topologyNodePath.size() - 1)) {
                opt.add(P_IJ == 1);// Fix the placement of source and sink
                P_IJ_stored.emplace_back(P_IJ);
            } else if (i == 0 && j != 0) {
                opt.add(P_IJ == 0);
                P_IJ_stored.emplace_back(P_IJ);
            } else {
                // The binary decision on whether to place or not, hence we constrain it to be either 0 or 1
                // Adding a z3 expression as an additional constraint to store the placement possibilities' conditions while maintaining the correct order of operator nodes.
                z3::expr check_previous_expression =
                    (P_IJ_stored[0] == 1);// possible use of first topology node as constraint to ensure right order
                // Iterating over all positions of topologyNodePath up to the current one (j) and adding them to the storing variable
                for (uint64_t k = 1; k <= j; ++k) {
                    check_previous_expression = check_previous_expression || (P_IJ_stored[k] == 1);
                }
                // Either P_IJ is 0 and the operator node is not be placed at current topology position (first condition)
                // or operator node is 1 and then placed at current topology node but only if the previous operator node is set at the same or a previous topology node (second condition)
                opt.add(P_IJ == 0 || (P_IJ == 1 && check_previous_expression));
                // Add the current state of P_IJ to the vector that is storing all possible operator node placements
                P_IJ_stored.emplace_back(P_IJ);
            }

            placementVariable.insert(std::make_pair(variableID, P_IJ));
            sum_i = sum_i + P_IJ;

            // add to node utilization
            int slots;
            if (!operatorNode->hasProperty("cost")) {
                slots = getDefaultOperatorCost(operatorNode);
            } else {
                std::any prop = operatorNode->getProperty("cost");
                slots = std::any_cast<int>(prop);
            }

            auto iterator = nodeUtilizationMap.find(topologyID);
            if (iterator != nodeUtilizationMap.end()) {
                iterator->second = iterator->second + slots * P_IJ;
            } else {
                // utilization of a node = slots (i.e. computing cost of operator) * placement variable
                nodeUtilizationMap.insert(std::make_pair(topologyID, slots * P_IJ));
            }

            // add distance to root (positive part of distance equation)
            double M = nodeMileageMap[topologyID];
            D_i = D_i + z3Context->real_val(std::to_string(M).c_str()) * P_IJ;
        }
        operatorDistanceMap.insert(std::make_pair(operatorID, D_i));
        // add constraint that operator is placed exactly once on topology path
        opt.add(sum_i == 1);
    }
}

bool ILPStrategy::pinOperators(z3::model& z3Model, std::map<std::string, z3::expr>& placementVariables) {
    for (const auto& placementMapping : placementVariables) {
        auto key = placementMapping.first;
        uint64_t operatorId = std::stoi(key.substr(0, key.find(KEY_SEPARATOR)));
        uint64_t topologyNodeId = std::stoi(key.substr(key.find(KEY_SEPARATOR) + 1));

        if (z3Model.eval(placementMapping.second).get_numeral_int() == 1) {
            NES_DEBUG("Pinning operator with ID {}", operatorId);
            //Pin the operator to the location identified by ILP algorithm
            auto logicalOperator = operatorMap[operatorId];
            logicalOperator->addProperty(PINNED_WORKER_ID, topologyNodeId);
        }
    }
    return true;
}

double ILPStrategy::getOverUtilizationCostWeight() { return this->overUtilizationCostWeight; }

double ILPStrategy::getNetworkCostWeight() { return this->networkCostWeight; }

void ILPStrategy::setOverUtilizationWeight(double weight) { this->overUtilizationCostWeight = weight; }

void ILPStrategy::setNetworkCostWeight(double weight) { this->networkCostWeight = weight; }

//FIXME: in #1422. This need to be defined better as at present irrespective of operator location we are returning always the default value
double ILPStrategy::getDefaultOperatorOutput(const LogicalOperatorPtr& operatorNode) {

    double dmf = 1;
    double input = 10;
    if (operatorNode->instanceOf<SinkLogicalOperator>()) {
        dmf = 0;
    } else if (operatorNode->instanceOf<LogicalFilterOperator>()) {
        dmf = .5;
    } else if (operatorNode->instanceOf<LogicalMapOperator>()) {
        dmf = 2;
    } else if (operatorNode->instanceOf<SourceLogicalOperator>()) {
        input = 100;
    }
    return dmf * input;
}

//FIXME: in #1422. This need to be defined better as at present irrespective of operator location we are returning always the default value
int ILPStrategy::getDefaultOperatorCost(const LogicalOperatorPtr& operatorNode) {

    if (operatorNode->instanceOf<SinkLogicalOperator>()) {
        return 0;
    } else if (operatorNode->instanceOf<LogicalFilterOperator>()) {
        return 1;
    } else if (operatorNode->instanceOf<LogicalMapOperator>() || operatorNode->instanceOf<LogicalJoinOperator>()
               || operatorNode->instanceOf<LogicalUnionOperator>()) {
        return 2;
    } else if (operatorNode->instanceOf<LogicalProjectionOperator>()) {
        return 1;
    } else if (operatorNode->instanceOf<SourceLogicalOperator>()) {
        return 0;
    }
    return 2;
}

}// namespace NES::Optimizer