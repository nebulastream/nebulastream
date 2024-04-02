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
#include <Catalogs/Topology/PathFinder.hpp>
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
        std::unordered_map<OperatorId, LogicalOperatorPtr> operatorIdToCopiedOperatorMap;
        std::map<uint64_t, double> nodeMileageMap = computeMileage(copy.copiedPinnedDownStreamOperators);

        // 2. Construct the placementVariable, compute distance, utilization and mileages
        for (const auto& pinnedUpStreamOperator : copy.copiedPinnedUpStreamOperators) {

            //2.1 Find all path between pinned upstream and downstream operators
            std::vector<NodePtr> operatorPath;
            std::queue<LogicalOperatorPtr> toProcess;

            operatorPath.push_back(pinnedUpStreamOperator);
            toProcess.push(pinnedUpStreamOperator);
            operatorIdToCopiedOperatorMap[pinnedUpStreamOperator->getId()] = pinnedUpStreamOperator;
            while (!toProcess.empty()) {
                auto operatorToProcess = toProcess.front();
                toProcess.pop();
                //Before further processing please identify if the operator to be processed is among the collection of pinned downstream operators
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
                    auto downstreamLogicalOperator = downstreamOperator->as_if<LogicalOperator>();
                    operatorIdToCopiedOperatorMap[downstreamLogicalOperator->getId()] = downstreamLogicalOperator;

                    // Only include unplaced operators in the path
                    if (downstreamLogicalOperator->getOperatorState() != OperatorState::PLACED) {
                        operatorPath.push_back(downstreamOperator);
                        toProcess.push(downstreamLogicalOperator);
                        unplacedDownStreamOperatorCount++;
                    }
                }
            }

            auto topologyLockedPath = std::vector<TopologyNodeWLock>();
            for (auto workerId : workerNodeIdsInBFS) {
                auto topologyNode = lockedTopologyNodeMap.find(workerId);
                if (topologyNode != lockedTopologyNodeMap.end()) {
                    // Recteate topological path
                    topologyLockedPath.emplace_back(topologyNode->second);
                }
            }

            //2.3 Add constraints to Z3 solver and compute operator distance, node utilization, and node mileage map
            addConstraints(opt,
                           pinnedUpStreamOperators,
                           pinnedDownStreamOperators,
                           topologyLockedPath,
                           placementVariables,
                           operatorPositionMap,
                           nodeUtilizationMap,
                           nodeMileageMap);
        }

        // 3. Calculate the network cost. (Network cost = sum over all operators (output of operator * distance of operator))
        auto costNet = z3Context->int_val(0);// initialize the network cost with 0

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

                    auto distance = operatorPositionMap.find(downStreamOperatorId)->second - position;
                    NES_DEBUG("Distance of {} to {} is: {}", operatorID, downStreamOperatorId, distance.to_string());
                    double output;
                    if (!logicalOperator->hasProperty("output")) {
                        NES_DEBUG("Getting default operator output");
                        output = getDefaultOperatorOutput(logicalOperator);
                    } else {
                        std::any prop = logicalOperator->getProperty("output");
                        output = std::any_cast<double>(prop);
                        NES_DEBUG("Property output of {} is: {}", logicalOperator->getId(), output);
                    }
                    //Summing up the amount of data multiplied by the distance to the position of the next operator and adding it to already summed up values of the same kind
                    costNet = costNet + z3Context->real_val(std::to_string(output).c_str()) * distance;
                }
            }
        }
        NES_DEBUG("costNet: {}", costNet.to_string());

        // 4. Calculate the node over-utilization cost.
        // Over-utilization cost = sum of the over-utilization of all nodes
        auto overUtilizationCost = z3Context->int_val(0);// initialize the over-utilization cost with 0
        for (auto const& [topologyID, utilization] : nodeUtilizationMap) {
            std::string overUtilizationId = "S" + std::to_string(topologyID);
            auto currentOverUtilization = z3Context->int_const(overUtilizationId.c_str());// an integer expression of the slack

            // Obtain the available slot in the current node
            TopologyNodePtr topologyNode = workerIdToTopologyNodeMap[topologyID]->as<TopologyNode>();
            auto availableSlot = topologyNode->getAvailableResources();

            opt.add(currentOverUtilization >= 0);// we only penalize over-utilization, hence its value should be >= 0.
            opt.add(utilization - currentOverUtilization <= availableSlot);// formula for the over-utilization

            overUtilizationCost = overUtilizationCost + currentOverUtilization;
        }

        auto weightOverUtilization = z3Context->real_val(std::to_string(this->overUtilizationCostWeight).c_str());
        auto weightNetwork = z3Context->real_val(std::to_string(this->networkCostWeight).c_str());

        NES_DEBUG("Values of weightNetwork, costNet, weightOverUtilization,  overUtilizationCost:");
        NES_DEBUG("WeightNetwork: {} \n costNet: {} \n weightOverUtilization: {} \n overUtilizationCost:{}",
                  weightNetwork.to_string(),
                  costNet.to_string(),
                  weightOverUtilization.to_string(),
                  overUtilizationCost.to_string());

        // 5. Optimize ILP problem and print solution.
        opt.minimize(weightNetwork * costNet
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
        NES_INFO("Solver found solution with cost: {}", z3Model.eval(costNet).get_decimal_string(4));

        // 7. Pick the solution which has placement decision of 1, i.e., the ILP decide to place the operator in that node
        std::map<OperatorPtr, TopologyNodePtr> operatorToTopologyNodeMap;
        for (auto const& [topologyID, P] : placementVariables) {
            if (z3Model.eval(P).get_numeral_int() == 1) {// means we place the operator in the node
                uint64_t operatorId = std::stoi(topologyID.substr(0, topologyID.find(',')));
                uint64_t topologyNodeId = std::stoi(topologyID.substr(topologyID.find(',') + 1));

                auto operatorIdToCopiedOperator = operatorIdToCopiedOperatorMap.find(operatorId);
                LogicalOperatorPtr logicalOperator;
                if (operatorIdToCopiedOperator != operatorIdToCopiedOperatorMap.end()) {
                    logicalOperator = operatorIdToCopiedOperator->second;
                }
                TopologyNodePtr topologyNode = workerIdToTopologyNodeMap[topologyNodeId];
                if (!logicalOperator->hasProperty(PINNED_WORKER_ID)) {
                    logicalOperator->addProperty(PINNED_WORKER_ID, topologyNodeId);
                }
                // collect the solution to operatorToTopologyNodeMap
                operatorToTopologyNodeMap.insert(std::make_pair(logicalOperator, topologyNode));
            }
        }

        NES_INFO("Solver found solution with cost: {}", z3Model.eval(costNet).get_decimal_string(4));
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

std::map<uint64_t, double> ILPStrategy::computeMileage(const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators) {
    std::map<uint64_t, double> mileageMap;// (topologyId, M)
    // populate the distance map
    for (const auto& pinnedDownStreamOperator : pinnedDownStreamOperators) {
        auto nodeId = std::any_cast<uint64_t>(pinnedDownStreamOperator->getProperty(PINNED_WORKER_ID));
        auto topologyNode = workerIdToTopologyNodeMap[nodeId];
        computeDistance(topologyNode, mileageMap);
    }
    return mileageMap;
}

void ILPStrategy::computeDistance(const TopologyNodePtr& node, std::map<uint64_t, double>& mileages) {
    uint64_t topologyID = node->getId();
    auto& children = node->getChildren();
    if (children.empty()) {
        mileages[topologyID] = 0.0;
        return;
    }

    TopologyNodePtr child = children[0]->as<TopologyNode>();
    uint64_t childID = child->getId();
    if (mileages.find(childID) == mileages.end()) {
        computeDistance(child, mileages);
    }
    mileages[topologyID] = 1.0 / node->getLinkProperty(childID)->bandwidth + mileages[childID];
}

void ILPStrategy::addConstraints(z3::optimize& opt,
                                 const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                 const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                 std::vector<TopologyNodeWLock>& topologyNodePath,
                                 std::map<std::string, z3::expr>& placementVariables,
                                 std::map<OperatorId, z3::expr>& operatorDistanceMap,
                                 std::map<uint64_t, z3::expr>& nodeUtilizationMap,
                                 std::map<uint64_t, double>& nodeMileageMap) {

    for (auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        auto pID = pinnedUpStreamOperator->getId();
        //if operator is already placed we move to its downstream operators
        if (pinnedUpStreamOperator->getOperatorState() == OperatorState::PLACED) {
            NES_DEBUG("Skip: Operator {} is already placed and thus skipping placement of it. Continuing with placing its "
                      "downstream operators",
                      pID);

            //Place all its downstream nodes
            for (auto& downStreamNode : pinnedUpStreamOperator->getParents()) {
                if (!(downStreamNode->as<LogicalOperator>()->getOperatorState() == OperatorState::PLACED)) {
                    identifyPinningLocation(downStreamNode->as<LogicalOperator>(),
                                            topologyNodePath,
                                            opt,
                                            placementVariables,
                                            pinnedDownStreamOperators,
                                            operatorDistanceMap,
                                            nodeUtilizationMap,
                                            nodeMileageMap);
                } else {
                    NES_DEBUG("Skipping downstream operator with id {} because it is already placed.",
                              downStreamNode->as<LogicalOperator>()->getId());
                }
            }
        } else {
            identifyPinningLocation(pinnedUpStreamOperator,
                                    topologyNodePath,
                                    opt,
                                    placementVariables,
                                    pinnedDownStreamOperators,
                                    operatorDistanceMap,
                                    nodeUtilizationMap,
                                    nodeMileageMap);
        }
    }
}

void ILPStrategy::identifyPinningLocation(const LogicalOperatorPtr& currentOperatorNode,
                                          std::vector<TopologyNodeWLock>& topologyNodePath,
                                          z3::optimize& opt,
                                          std::map<std::string, z3::expr>& placementVariable,
                                          const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                          std::map<OperatorId, z3::expr>& operatorDistanceMap,
                                          std::map<uint64_t, z3::expr>& nodeUtilizationMap,
                                          std::map<uint64_t, double>& nodeMileageMap) {
    auto operatorNode = currentOperatorNode->as<LogicalOperator>();
    OperatorId operatorID = operatorNode->getId();
    NES_DEBUG("Handling operatorNode {} with id: {}", operatorNode->toString(), operatorID);

    //node was already treated
    if (operatorMap.find(operatorID) != operatorMap.end()) {
        NES_DEBUG("Skipping: Operator {} was found on operatorMap", operatorID);
        // Initialize the path constraint variable to 0
        auto pathConstraint = z3Context->int_val(0);
        for (uint64_t j = 0; j < topologyNodePath.size(); j++) {
            TopologyNodePtr topologyNode = (*topologyNodePath[j])->operator->()->as<TopologyNode>();
            uint64_t topologyID = topologyNode->getId();
            std::string variableID = std::to_string(operatorID) + KEY_SEPARATOR + std::to_string(topologyID);
            auto iter = placementVariable.find(variableID);
            if (iter != placementVariable.end()) {
                pathConstraint = pathConstraint + iter->second;
            }
        }
        opt.add(pathConstraint == 1);
        return;
    }

    this->operatorMap[operatorID] = operatorNode;
    // Fill the placement variable, utilization, and distance map
    auto sum_i = this->z3Context->int_val(0);
    auto D_i = this->z3Context->int_val(0);

    //for the current operatorNode we iterate here through all topologyNodes and calculate costs, possibility of placement etc.
    for (uint64_t j = 0; j < topologyNodePath.size(); j++) {

        TopologyNodePtr topologyNode = (*topologyNodePath[j])->operator->()->as<TopologyNode>();
        uint64_t topologyID = topologyNode->getId();
        NES_DEBUG("Handling topologyNode with id: {}", topologyID);

        //create placement variable and constraint to {0,1}
        std::string variableID = std::to_string(operatorID) + this->KEY_SEPARATOR + std::to_string(topologyID);
        auto P_IJ = this->z3Context->int_const(variableID.c_str());
        if (operatorNode->getOperatorState() == OperatorState::PLACED) {
            break;
        }
        if ((operatorNode->instanceOf<SinkLogicalOperator>() && j == topologyNodePath.size() - 1)
            || (operatorNode->instanceOf<SourceLogicalOperator>() && j == 0)) {
            NES_DEBUG("Handling a source or a sink on right topology node");
            opt.add(P_IJ == 1);//placement of all sources and final sinks is fixed

        } else if ((operatorNode->instanceOf<SinkLogicalOperator>() && j != topologyNodePath.size() - 1)
                   || (operatorNode->instanceOf<SourceLogicalOperator>() && j != 0)) {
            NES_DEBUG("Handling a source or a sink on wrong topology node");
            opt.add(P_IJ == 0);//source or sink on wrong topologyNode mustn't be placed
        } else {
            NES_DEBUG("Handling any other operator than source/sink");

            // Initialize P_IJ_stored
            int sizeOfPIJ_stored_out = operatorNode->getChildren().size() + 1;//all children + operatorNode itself
            int sizeOfPIJ_stored_in = j + 1;
            std::vector<std::vector<z3::expr>> P_IJ_stored(sizeOfPIJ_stored_out,
                                                           std::vector<z3::expr>(sizeOfPIJ_stored_in, P_IJ));

            // Create constraint
            NES_DEBUG("Placing operator {} on topology {}", operatorID, topologyID);
            auto variableID_new = std::to_string(operatorID) + this->KEY_SEPARATOR + std::to_string(topologyID);
            auto PIJ = this->z3Context->int_const(variableID_new.c_str());
            uint64_t counter = 1;
            z3::expr checkChildren = (PIJ == 1);

            // Iterating over all children of the current operator node to make sure that children should be placed on a
            // lower topology node than the current operatorNode (or the same)
            for (auto child : operatorNode->getChildren()) {
                if (!child->instanceOf<SourceLogicalOperator>()) {
                    int childID = child->as<LogicalOperator>()->getId();
                    auto id = std::to_string(childID) + this->KEY_SEPARATOR
                        + std::to_string((*topologyNodePath[j])->operator->()->as<TopologyNode>()->getId());
                    auto P_IJ_child = this->z3Context->int_const(id.c_str());
                    z3::expr checkTopologyNodes = (P_IJ_child == 1);

                    // Iterating over all positions of topologyNodePath up to the current one
                    for (uint64_t n = 0; n < j;
                         ++n) {// n < j here and n == j in initialising checkTopologyNodes means we actually check n <= j
                        auto variableID_loop = std::to_string(childID) + this->KEY_SEPARATOR
                            + std::to_string((*topologyNodePath[n])->operator->()->as<TopologyNode>()->getId());
                        auto P_IJ_nodes = this->z3Context->int_const(variableID_loop.c_str());
                        P_IJ_stored[counter][n] = P_IJ_nodes;
                        // see if for all the children c of the operator nodes one topologyNode (n) before the current
                        // one or the current one exists where it (child of operatorNode) is placed
                        checkTopologyNodes = (checkTopologyNodes || (P_IJ_stored[counter][n] == 1));
                    }
                    checkChildren = (checkChildren && checkTopologyNodes);
                    counter++;
                }
            }

            //either we do NOT place current operatorNode on current topologyNode (first half)
            //or we place it but then for all children must be true: they are placed not after the current operatorNode
            //add that condition to the optimizer
            opt.add(PIJ == 0 || checkChildren);
        }
        placementVariable.insert(std::make_pair(variableID, P_IJ));
        sum_i = sum_i + P_IJ;
        NES_DEBUG("placement variables for operator {} look like this: ", operatorID);
        for (auto e : placementVariable) {
            NES_DEBUG("{}", e.second.to_string());
        }
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

    }//end of iteration through topologyNodes

    operatorDistanceMap.insert(std::make_pair(operatorID, D_i));
    std::stringstream v;
    v << D_i << std::endl;
    NES_DEBUG("Operator {} inserted into operatorPositionMap with value {}", operatorID, v.str());

    // add constraint that operator is placed exactly once on topology path
    opt.add(sum_i == 1);

    // recursive call for all parents
    for (auto parent : currentOperatorNode->getParents()) {
        NES_DEBUG("parent for {} is {}", operatorID, parent->as<LogicalOperator>()->getId());
        if (!(currentOperatorNode->getOperatorState() == OperatorState::PLACED)) {
            identifyPinningLocation(parent->as<LogicalOperator>(),
                                    topologyNodePath,
                                    opt,
                                    placementVariable,
                                    pinnedDownStreamOperators,
                                    operatorDistanceMap,
                                    nodeUtilizationMap,
                                    nodeMileageMap);
        } else {
            NES_DEBUG("Skipping operator with id {} because it's already placed.", parent->as<LogicalOperator>()->getId());
        }
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