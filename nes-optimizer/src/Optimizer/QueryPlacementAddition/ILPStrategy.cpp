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
#include <Util/CopiedPinnedOperators.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <charconv>
#include <z3++.h>

namespace NES::Optimizer {

BasePlacementStrategyPtr ILPStrategy::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                             const TopologyPtr& topology,
                                             const TypeInferencePhasePtr& typeInferencePhase,
                                             PlacementAmendmentMode placementAmendmentMode) {
    z3::config cfg;
    cfg.set("timeout", 1000000);
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

static std::pair<OperatorId, WorkerId> deconstructTopologyId(std::string_view topologyId, const char* separator) {
    uint64_t operatorId;
    uint64_t workerId;
    auto operatorIdSubstring = topologyId.substr(0, topologyId.find(separator));
    auto executionNodeSubstr = topologyId.substr(topologyId.find(separator) + 1);

    if (std::from_chars(operatorIdSubstring.data(), operatorIdSubstring.data() + operatorIdSubstring.length(), operatorId).ec
        == std::errc::invalid_argument) {
        NES_THROW_RUNTIME_ERROR("Could not parse OperatorId: " << topologyId);
    }
    if (std::from_chars(executionNodeSubstr.data(), executionNodeSubstr.data() + executionNodeSubstr.length(), workerId).ec
        == std::errc::invalid_argument) {
        NES_THROW_RUNTIME_ERROR("Could not parse WorkerId: " << topologyId);
    }

    return {OperatorId(operatorId), WorkerId(workerId)};
}

PlacementAdditionResult ILPStrategy::updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                                               const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                                               const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                                               DecomposedQueryPlanVersion querySubPlanVersion) {

    try {
        NES_INFO("Performing placement of the input query plan with id {}", sharedQueryId);
        NES_INFO("Processing {} num of upstream and {} num of downstream optrs",
                 pinnedUpStreamOperators.size(),
                 pinnedDownStreamOperators.size());

        // 1. Create copy of the query plan
        auto copy =
            CopiedPinnedOperators::create(pinnedUpStreamOperators, pinnedDownStreamOperators, operatorIdToOriginalOperatorMap);

        // 2. Find the path where operators need to be placed
        performPathSelection(copy.copiedPinnedUpStreamOperators, copy.copiedPinnedDownStreamOperators);

        z3::optimize opt(*z3Context);
        std::map<std::string, z3::expr> placementVariables;
        std::map<OperatorId, z3::expr> operatorPositionMap;
        std::map<WorkerId, z3::expr> nodeUtilizationMap;
        std::unordered_map<OperatorId, LogicalOperatorPtr> operatorIdToCopiedOperatorMap;
        auto nodeMileageMap = computeMileage(copy.copiedPinnedDownStreamOperators);

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

            //2.3 Check that all costs that are necessary for z3 solver are specified
            for (const auto& pinnedDownstreamOperator : copy.copiedPinnedDownStreamOperators) {
                if (!pinnedDownstreamOperator->hasProperty("cost") || !pinnedDownstreamOperator->hasProperty("output")) {
                    assignOperatorDefaultProperties(pinnedDownstreamOperator);
                }
            }

            //2.4 Add constraints to Z3 solver and compute operator distance, node utilization, and node mileage map

            auto newNodeMileageMap = computeMileage(copy.copiedPinnedDownStreamOperators);

            addConstraints(opt,
                           copy.copiedPinnedUpStreamOperators,
                           copy.copiedPinnedDownStreamOperators,
                           placementVariables,
                           operatorPositionMap,
                           nodeUtilizationMap,
                           newNodeMileageMap);
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
                    std::any prop = logicalOperator->getProperty("output");
                    double output = std::any_cast<double>(prop);
                    NES_DEBUG("Property output of {} is: {}", logicalOperator->getId(), output);
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
            std::string overUtilizationId = fmt::format("S{}", topologyID);
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
        z3::check_result result;
        uint8_t maxRetryCount = 4;
        uint8_t retryCount = 0;
        while (((result = opt.check()) == z3::unknown) && retryCount < maxRetryCount) {
            retryCount++;
            NES_ERROR("Solver retry number {}", retryCount)
        }

        if (z3::sat != result) {
            NES_ERROR("Solver failed for {} with {} after {} retries.", sharedQueryId, result, retryCount)
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
                auto [operatorId, nodeId] = deconstructTopologyId(topologyID, KEY_SEPARATOR);

                auto operatorIdToCopiedOperator = operatorIdToCopiedOperatorMap.find(operatorId);
                LogicalOperatorPtr logicalOperator;
                if (operatorIdToCopiedOperator != operatorIdToCopiedOperatorMap.end()) {
                    logicalOperator = operatorIdToCopiedOperator->second;
                }
                TopologyNodePtr topologyNode = workerIdToTopologyNodeMap[nodeId];
                if (!logicalOperator->hasProperty(PINNED_WORKER_ID)) {
                    logicalOperator->addProperty(PINNED_WORKER_ID, nodeId);
                    NES_DEBUG("Optr {} is pinned on {}",
                              logicalOperator->toString(),
                              std::any_cast<WorkerId>(logicalOperator->getProperty(PINNED_WORKER_ID)));
                } else {
                    NES_DEBUG("Optr {} is pinned on {}",
                              logicalOperator->toString(),
                              std::any_cast<WorkerId>(logicalOperator->getProperty(PINNED_WORKER_ID)));
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

std::map<WorkerId, double> ILPStrategy::computeMileage(const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators) {
    std::map<WorkerId, double> mileageMap;// (topologyId, M)
    // populate the distance map
    for (const auto& pinnedDownStreamOperator : pinnedDownStreamOperators) {
        auto nodeId = std::any_cast<WorkerId>(pinnedDownStreamOperator->getProperty(PINNED_WORKER_ID));
        auto topologyNode = workerIdToTopologyNodeMap[nodeId];
        computeDistance(topologyNode, mileageMap);
    }
    return mileageMap;
}

void ILPStrategy::computeDistance(const TopologyNodePtr& node, std::map<WorkerId, double>& mileages) {
    auto topologyID = node->getId();
    auto& children = node->getChildren();
    if (children.empty()) {
        mileages[topologyID] = 0.0;
        return;
    }

    // if the current node is not a root node, recursively compute the mileage of its first parent
    // assuming a node only has a single parent
    TopologyNodePtr child = children[0]->as<TopologyNode>();
    auto childID = child->getId();
    if (mileages.find(childID) == mileages.end()) {
        computeDistance(child, mileages);
    }
    if (!node->getLinkProperty(childID) || !node->getLinkProperty(childID)->bandwidth) {
        auto linkProperty = std::make_shared<LinkProperty>(512, 100);
        node->addLinkProperty(childID, linkProperty);
    }
    mileages[topologyID] = 1.0 / node->getLinkProperty(childID)->bandwidth + mileages[childID];
}

void ILPStrategy::addConstraints(z3::optimize& opt,
                                 const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                 const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                 std::map<std::string, z3::expr>& placementVariables,
                                 std::map<OperatorId, z3::expr>& operatorDistanceMap,
                                 std::map<WorkerId, z3::expr>& nodeUtilizationMap,
                                 std::map<WorkerId, double>& nodeMileageMap) {

    // Compute a map for potential pinning locations by doing static analysis
    std::unordered_map<OperatorId, std::vector<WorkerId>> potentialOperatorPinningLocation;

    // Temp container for iteration
    std::queue<LogicalOperatorPtr> operatorsToProcess;
    for (auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        operatorsToProcess.emplace(pinnedUpStreamOperator);
    }

    //FIXME (#4958): there is few short coming of the trailing algorithm (More may exists):
    // 1. Possible missing pinning locations can occur as the current implementation only analyzes the upstream operators to
    // find the possible locations. For example, if an operator is connected to a Union operator serving multiple downstream
    // operators located on different nodes. This union operator can only be placed on topology nodes that can reach the downstream
    // operators. Any further connected downstream operator to this union operator will, therefore, also be constrained to the
    // possible pinning locations of the union. This can result in missed pinning opportunities.
    // 2. It assumes that connected most upstream and downstream operators are always pinned (this is generally an assumption in
    // our system. I can not think of an example where this won't be the case).

    // Iterate over all operators and find the possible pining locations
    while (!operatorsToProcess.empty()) {

        auto operatorToProcess = operatorsToProcess.front();
        operatorsToProcess.pop();
        auto operatorId = operatorToProcess->getId();

        if (operatorToProcess->hasProperty(PINNED_WORKER_ID)) {
            auto pinnedWorkerId = std::any_cast<WorkerId>(operatorToProcess->getProperty(PINNED_WORKER_ID));
            potentialOperatorPinningLocation[operatorToProcess->getId()] = {pinnedWorkerId};
        } else {// Compute the possible pinning locations

            bool upstreamAnalyzed = true;
            bool connectedToPinnedOperator = false;
            // 1. Check if all upstream operators are analyzed and their potential pinning locations are identified.
            // 2. Check if connected to pinned upstream operator.
            auto& upstreamOperators = operatorToProcess->getChildren();
            for (const auto& upstreamOperator : upstreamOperators) {
                auto logicalOperator = upstreamOperator->as<LogicalOperator>();
                auto upstreamOperatorId = logicalOperator->getId();
                if (!potentialOperatorPinningLocation.contains(upstreamOperatorId)) {
                    upstreamAnalyzed = false;
                    break;
                }
                if (logicalOperator->hasProperty(PINNED_WORKER_ID)) {
                    connectedToPinnedOperator = true;
                }
            }

            if (!upstreamAnalyzed) {
                continue;
            }

            if (connectedToPinnedOperator) {
                // Fetch all connected upstream and downstream operators
                auto connectedPinnedUpstreamNodes = operatorToProcess->getAllLeafNodes();
                auto connectedPinnedDownstreamNodes = operatorToProcess->getAllRootNodes();

                std::vector<TopologyNodePtr> upstreamTopologyNodes;
                // Fetch the pinned workerId
                for (const auto& connectedPinnedUpstreamNode : connectedPinnedUpstreamNodes) {
                    if (!connectedPinnedUpstreamNode->as_if<LogicalOperator>()->hasProperty(PINNED_WORKER_ID)) {
                        NES_ERROR("The connected leaf nodes should always carry pinning location");
                        throw std::logic_error("The connected leaf nodes should always carry pinning location");
                    }
                    auto workerId = std::any_cast<WorkerId>(
                        connectedPinnedUpstreamNode->as_if<LogicalOperator>()->getProperty(PINNED_WORKER_ID));

                    // Check if the topology node was already added
                    bool alreadyAdded = std::find_if(upstreamTopologyNodes.begin(),
                                                     upstreamTopologyNodes.end(),
                                                     [&](const TopologyNodePtr& topologyNode) {
                                                         return topologyNode->getId() == workerId;
                                                     })
                        != upstreamTopologyNodes.end();
                    if (!alreadyAdded) {
                        upstreamTopologyNodes.emplace_back(workerIdToTopologyNodeMap[workerId]);
                    }
                }

                std::vector<TopologyNodePtr> downstreamTopologyNodes;
                // Fetch the pinned workerId
                for (const auto& connectedPinnedDownstreamNode : connectedPinnedDownstreamNodes) {
                    if (!connectedPinnedDownstreamNode->as_if<LogicalOperator>()->hasProperty(PINNED_WORKER_ID)) {
                        NES_ERROR("The connected root nodes should always carry pinning location");
                        throw std::logic_error("The connected root nodes should always carry pinning location");
                    }
                    auto workerId = std::any_cast<WorkerId>(
                        connectedPinnedDownstreamNode->as_if<LogicalOperator>()->getProperty(PINNED_WORKER_ID));
                    // Check if the topology node was already added
                    bool alreadyAdded = std::find_if(downstreamTopologyNodes.begin(),
                                                     downstreamTopologyNodes.end(),
                                                     [&](const TopologyNodePtr& topologyNode) {
                                                         return topologyNode->getId() == workerId;
                                                     })
                        != downstreamTopologyNodes.end();
                    if (!alreadyAdded) {
                        downstreamTopologyNodes.emplace_back(workerIdToTopologyNodeMap[workerId]);
                    }
                }

                // Find common topology nodes
                auto commonTopologyNodes = pathFinder->findNodesBetween(upstreamTopologyNodes, downstreamTopologyNodes);

                // Possible pinning locations
                std::vector<WorkerId> possiblePinningLocations;

                // If only one upstream topology node then add the upstream topology node to the possible pinning location
                if (upstreamTopologyNodes.size() == 1) {
                    possiblePinningLocations.emplace_back(upstreamTopologyNodes[0]->getId());
                }

                // Iterate over common topology nodes to compute remaining possible pinning locations
                for (const auto& commonWorkerNode : commonTopologyNodes) {
                    possiblePinningLocations.emplace_back(commonWorkerNode->getId());
                }
                // Store potential pinning locations
                potentialOperatorPinningLocation[operatorId] = possiblePinningLocations;
            } else {// Compute the possible locations based on the connected upstream operators

                std::vector<WorkerId> possiblePinningLocations;
                for (const auto& upstreamOperator : upstreamOperators) {
                    // If possible pinning location not computed then assign the possible locations of the upstream operator
                    if (possiblePinningLocations.empty()) {
                        possiblePinningLocations =
                            potentialOperatorPinningLocation[upstreamOperator->as_if<LogicalOperator>()->getId()];
                    } else {// Compute intersection with upstream operator to find common worker nodes
                        // Note: we are not using std::set_intersection to preserve the workerId order

                        // Fetch the possible pinning locations of the upstream operator
                        std::vector<WorkerId> upstreamOperatorPossiblePinningLocations =
                            potentialOperatorPinningLocation[upstreamOperator->as_if<LogicalOperator>()->getId()];

                        // Remove the element from the possible pinning locations if the worker id is not listed
                        // in the possible pinning location of the upstream operator
                        possiblePinningLocations.erase(
                            std::remove_if(possiblePinningLocations.begin(),
                                           possiblePinningLocations.end(),
                                           [&](WorkerId workerId) {
                                               // return true if element not in the upstream Operator's Possible Pinning locations
                                               return std::find(upstreamOperatorPossiblePinningLocations.begin(),
                                                                upstreamOperatorPossiblePinningLocations.end(),
                                                                workerId)
                                                   == upstreamOperatorPossiblePinningLocations.end();
                                           }),
                            possiblePinningLocations.end());
                    }
                }
                // Store potential pinning locations
                potentialOperatorPinningLocation[operatorId] = possiblePinningLocations;
            }
        }

        // Add to the list of operators to be processed
        const auto& downstreamOperators = operatorToProcess->getParents();
        std::for_each(downstreamOperators.begin(), downstreamOperators.end(), [&](const NodePtr& operatorNode) {
            operatorsToProcess.emplace(operatorNode->as<LogicalOperator>());
        });
    }

    for (auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        auto pID = pinnedUpStreamOperator->getId();

        //if operator is already placed we move to its downstream operators
        if (pinnedUpStreamOperator->getOperatorState() == OperatorState::PLACED) {
            NES_DEBUG("Skip: Operator {} is already placed and thus skipping placement of it. Continuing with placing its "
                      "downstream operators",
                      pID);

            //Place all its downstream nodes
            for (auto& downStreamNode : pinnedUpStreamOperator->getParents()) {
                if (downStreamNode->as<LogicalOperator>()->getOperatorState() != OperatorState::PLACED) {
                    identifyPinningLocation(downStreamNode->as<LogicalOperator>(),
                                            opt,
                                            potentialOperatorPinningLocation,
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
                                    opt,
                                    potentialOperatorPinningLocation,
                                    placementVariables,
                                    pinnedDownStreamOperators,
                                    operatorDistanceMap,
                                    nodeUtilizationMap,
                                    nodeMileageMap);
        }
    }
}

void ILPStrategy::identifyPinningLocation(const LogicalOperatorPtr& currentOperatorNode,
                                          z3::optimize& opt,
                                          std::unordered_map<OperatorId, std::vector<WorkerId>> potentialOperatorPinningLocation,
                                          std::map<std::string, z3::expr>& placementVariable,
                                          const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                          std::map<OperatorId, z3::expr>& operatorDistanceMap,
                                          std::map<WorkerId, z3::expr>& nodeUtilizationMap,
                                          std::map<WorkerId, double>& nodeMileageMap) {

    auto operatorNode = currentOperatorNode->as<LogicalOperator>();
    OperatorId operatorID = operatorNode->getId();
    NES_DEBUG("Handling operatorNode {} with id: {}", operatorNode->toString(), operatorID);

    //node was already treated
    if (operatorMap.contains(operatorID)) {
        NES_DEBUG("Skipping: Operator {} was found on operatorMap", operatorID);
        // Initialize the path constraint variable to 0
        auto pathConstraint = z3Context->int_val(0);
        for (auto topologyID : workerNodeIdsInBFS) {
            std::string variableID = fmt::format("{}{}{}", operatorID, KEY_SEPARATOR, topologyID);
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

    //Only add constraints if the operator is not placed
    if (operatorNode->getOperatorState() != OperatorState::PLACED) {
        //Fetch the potential pinning locations
        auto potentialPinningLocations = potentialOperatorPinningLocation[operatorID];

        //for the current operatorNode we iterate here through all topologyNodes and calculate costs, possibility of placement etc.
        for (uint64_t j = 0; j < workerNodeIdsInBFS.size(); j++) {

            auto topologyID = workerNodeIdsInBFS[j];
            NES_DEBUG("Handling topologyNode with id: {}", topologyID);

            //create placement variable and constraint to {0,1}
            std::string variableID = fmt::format("{}{}{}", operatorID, KEY_SEPARATOR, topologyID);
            auto P_IJ = this->z3Context->int_const(variableID.c_str());

            bool canBePlaced = (std::find(potentialPinningLocations.begin(), potentialPinningLocations.end(), topologyID)
                                != potentialPinningLocations.end());

            if (operatorNode->instanceOf<SinkLogicalOperator>() || operatorNode->instanceOf<SourceLogicalOperator>()) {
                NES_DEBUG("Handling a source or a sink on right topology node");
                if (canBePlaced) {
                    opt.add(P_IJ == 1);
                } else {
                    opt.add(P_IJ == 0);
                }
            } else {
                NES_DEBUG("Handling any other operator than source/sink");

                // Initialize P_IJ_stored
                int sizeOfPIJ_stored_out = operatorNode->getChildren().size() + 1;//all children + operatorNode itself
                int sizeOfPIJ_stored_in = j + 1;
                std::vector P_IJ_stored(sizeOfPIJ_stored_out, std::vector<z3::expr>(sizeOfPIJ_stored_in, P_IJ));

                // Create constraint
                NES_DEBUG("Placing operator {} on topology {}", operatorID, topologyID);
                auto variableID_new = fmt::format("{}{}{}", operatorID, KEY_SEPARATOR, topologyID);
                auto PIJ = this->z3Context->int_const(variableID_new.c_str());
                uint64_t counter = 1;
                z3::expr operatorPinningConstraint = (PIJ == 0);
                if (canBePlaced) {
                    operatorPinningConstraint = (PIJ == 1);
                    // Iterating over all children of the current operator node to make sure that children should be placed on a
                    // lower topology node than the current operatorNode (or the same)
                    for (const auto& child : operatorNode->getChildren()) {
                        auto childID = child->as<LogicalOperator>()->getId();
                        auto id = fmt::format("{}{}{}", childID, KEY_SEPARATOR, workerNodeIdsInBFS[j]);
                        auto P_IJ_child = this->z3Context->int_const(id.c_str());

                        //Fetch the potential pinning locations
                        auto potentialUpstreamOperatorPinningLocations = potentialOperatorPinningLocation[childID];

                        z3::expr_vector upstreamOperatorPinningConstraints(*z3Context);
                        // Iterating over all positions of topologyNodePath up to the current one
                        for (uint64_t n = 0; n <= j; ++n) {
                            // n < j here and n == j in initialising checkTopologyNodes means we actually check n <= j
                            auto variableID_loop = fmt::format("{}{}{}", childID, KEY_SEPARATOR, workerNodeIdsInBFS[n]);
                            auto P_IJ_nodes = this->z3Context->int_const(variableID_loop.c_str());
                            P_IJ_stored[counter][n] = P_IJ_nodes;

                            bool canUpstreamOperatorBePlaced = (std::find(potentialUpstreamOperatorPinningLocations.begin(),
                                                                          potentialUpstreamOperatorPinningLocations.end(),
                                                                          workerNodeIdsInBFS[n])
                                                                != potentialUpstreamOperatorPinningLocations.end());
                            if (canUpstreamOperatorBePlaced) {
                                // see if for all the children c of the operator nodes one topologyNode (n) before the current
                                // one or the current one exists where it (child of operatorNode) is placed
                                upstreamOperatorPinningConstraints.push_back((P_IJ_stored[counter][n] == 1));
                            }
                        }
                        if (!upstreamOperatorPinningConstraints.empty()) {
                            operatorPinningConstraint =
                                (operatorPinningConstraint && z3::mk_or(upstreamOperatorPinningConstraints));
                        }
                        counter++;
                    }
                }
                //either we do NOT place current operatorNode on current topologyNode (first half)
                //or we place it but then for all children must be true: they are placed not after the current operatorNode
                //add that condition to the optimizer
                opt.add((P_IJ == 0) || operatorPinningConstraint);

                NES_DEBUG("Print the condition {}", (operatorPinningConstraint).to_string());
            }

            placementVariable.insert(std::make_pair(variableID, P_IJ));
            sum_i = sum_i + P_IJ;
            NES_DEBUG("placement variables for operator {} look like this: ", operatorID);
            for (auto e : placementVariable) {
                NES_DEBUG("{}", e.second.to_string());
            }
            // add to node utilization

            if (!operatorNode->hasProperty("cost")) {
                NES_ERROR("Cost for the operator id {} is missing", operatorNode->getId());
            }
            std::any prop = operatorNode->getProperty("cost");
            int slots = std::any_cast<int>(prop);

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
    }
    operatorDistanceMap.insert(std::make_pair(operatorID, D_i));
    std::stringstream v;
    v << D_i << std::endl;
    NES_DEBUG("Operator {} inserted into operatorPositionMap with value {}", operatorID, v.str());

    // add constraint that operator is placed exactly once on topology path
    opt.add(sum_i == 1);

    // recursive call for all parents
    for (const auto& parent : currentOperatorNode->getParents()) {
        NES_DEBUG("parent for {} is {}", operatorID, parent->as<LogicalOperator>()->getId());
        if (currentOperatorNode->getOperatorState() != OperatorState::PLACED) {
            identifyPinningLocation(parent->as<LogicalOperator>(),
                                    opt,
                                    potentialOperatorPinningLocation,
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
        auto [operatorId, topologyNodeId] = deconstructTopologyId(key, KEY_SEPARATOR);

        if (z3Model.eval(placementMapping.second).get_numeral_int() == 1) {
            NES_DEBUG("Pinning operator with ID {}", operatorId);
            //Pin the operator to the location identified by ILP algorithm
            auto logicalOperator = operatorMap[operatorId];
            logicalOperator->addProperty(PINNED_WORKER_ID, topologyNodeId);
        }
    }
    return true;
}

double ILPStrategy::getOverUtilizationCostWeight() const { return this->overUtilizationCostWeight; }

double ILPStrategy::getNetworkCostWeight() const { return this->networkCostWeight; }

void ILPStrategy::setOverUtilizationWeight(double weight) { this->overUtilizationCostWeight = weight; }

void ILPStrategy::setNetworkCostWeight(double weight) { this->networkCostWeight = weight; }

void ILPStrategy::assignOperatorDefaultProperties(const LogicalOperatorPtr& operatorNode) {
    int cost = 1;
    double dmf = 1;
    double input = 0;

    for (const auto& child : operatorNode->getChildren()) {
        LogicalOperatorPtr op = child->as<LogicalOperator>();
        assignOperatorDefaultProperties(op);
        std::any output = op->getProperty("output");
        input += std::any_cast<double>(output);
    }

    NodePtr nodePtr = operatorNode->as<Node>();
    if (operatorNode->instanceOf<SinkLogicalOperator>()) {
        dmf = 0;
        cost = 0;
    } else if (operatorNode->instanceOf<LogicalFilterOperator>()) {
        dmf = 0.5;
        cost = 1;
    } else if (operatorNode->instanceOf<LogicalMapOperator>()) {
        dmf = 2;
        cost = 2;
    } else if (operatorNode->instanceOf<LogicalJoinOperator>()) {
        cost = 2;
    } else if (operatorNode->instanceOf<LogicalUnionOperator>()) {
        cost = 2;
    } else if (operatorNode->instanceOf<LogicalProjectionOperator>()) {
        cost = 1;
    } else if (operatorNode->instanceOf<SourceLogicalOperator>()) {
        cost = 0;
        input = 100;
    }

    double output = input * dmf;
    operatorNode->addProperty("output", output);
    operatorNode->addProperty("cost", cost);
}

}// namespace NES::Optimizer
