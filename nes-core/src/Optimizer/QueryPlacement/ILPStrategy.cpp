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
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <z3++.h>
#include <iostream>
#include <string>
#include <cctype>

namespace NES::Optimizer {

std::unique_ptr<BasePlacementStrategy>
ILPStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase) {
    z3::config cfg;
    cfg.set("timeout", 1000);
    cfg.set("model", false);
    cfg.set("type_check", false);
    auto z3Context = std::make_shared<z3::context>(cfg);
    return std::make_unique<ILPStrategy>(ILPStrategy(globalExecutionPlan, topology, typeInferencePhase, z3Context));
}

ILPStrategy::ILPStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                         TopologyPtr topology,
                         TypeInferencePhasePtr typeInferencePhase,
                         z3::ContextPtr z3Context)
    : BasePlacementStrategy(globalExecutionPlan, topology, typeInferencePhase), z3Context(std::move(z3Context)) {}

bool ILPStrategy::updateGlobalExecutionPlan(QueryId queryId,
                                            FaultToleranceType faultToleranceType,
                                            LineageType lineageType,
                                            const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                            const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    NES_INFO("ILPStrategy: Performing placement of the input query plan with id {}", queryId);

    // 1. Find the path where operators need to be placed
    performPathSelection(pinnedUpStreamOperators, pinnedDownStreamOperators);

    z3::optimize opt(*z3Context);
    std::map<std::string, z3::expr> placementVariables;
    std::map<OperatorId, z3::expr> operatorPositionMap;
    std::map<uint64_t, z3::expr> nodeUtilizationMap;
    std::map<uint64_t, double> nodeMileageMap = computeMileage(pinnedUpStreamOperators);

    // 2. Construct the placementVariable, compute distance, utilization and mileages
    for (const auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {

        //2.1 Find all path between pinned upstream and downstream operators
        std::vector<NodePtr> operatorPath;
        operatorPath.push_back(pinnedUpStreamOperator);
        while (!operatorPath.back()->getParents().empty()) {

            //Before further processing please identify if the operator to be processed is among the collection of pinned downstream operators
            auto& operatorToProcess = operatorPath.back();
            auto isPinnedDownStreamOperator = std::find_if(
                pinnedDownStreamOperators.begin(),
                pinnedDownStreamOperators.end(),
                [operatorToProcess](const LogicalOperatorNodePtr& pinnedDownStreamOperator) {
                    return pinnedDownStreamOperator->getId() == operatorToProcess->as_if<LogicalOperatorNode>()->getId();
                });

            //Skip further processing if encountered pinned downstream operator
            if (isPinnedDownStreamOperator != pinnedDownStreamOperators.end()) {
                NES_DEBUG("ILPStrategy: Found pinned downstream operator. Skipping further downstream operators.");
                break;
            }

            //Look for the next downstream operators and add them to the path.
            auto downstreamOperators = operatorToProcess->getParents();

            if (downstreamOperators.empty()) {
                NES_ERROR("ILPStrategy: Unable to find pinned downstream operator.");
                return false;
            }

            uint16_t unplacedDownStreamOperatorCount = 0;
            for (auto& downstreamOperator : downstreamOperators) {

                // FIXME: (issue #2290) Assuming a tree structure, hence a node can only have a single parent. However, a query can have
                //  multiple sinks or parents.
                if (unplacedDownStreamOperatorCount > 1) {
                    NES_ERROR("ILPStrategy: Current implementation can not place plan with multiple downstream operators.");
                    return false;
                }

                // Only include unplaced operators in the path
                if (downstreamOperator->as_if<LogicalOperatorNode>()->getOperatorState() != OperatorState::PLACED) {
                    operatorPath.push_back(downstreamOperator);
                    unplacedDownStreamOperatorCount++;
                }
            }
        }

        //2.2 Find path between pinned upstream and downstream topology node
        auto upstreamPinnedNodeId = std::any_cast<uint64_t>(pinnedUpStreamOperator->getProperty(PINNED_NODE_ID));
        auto upstreamTopologyNode = topologyMap[upstreamPinnedNodeId];

        auto downstreamPinnedNodeId =
            std::any_cast<uint64_t>(operatorPath.back()->as_if<LogicalOperatorNode>()->getProperty(PINNED_NODE_ID));
        auto downstreamTopologyNode = topologyMap[downstreamPinnedNodeId];

        std::vector<TopologyNodePtr> topologyPath = topology->findPathBetween({upstreamTopologyNode}, {downstreamTopologyNode});

        while (!topologyPath.back()->getParents().empty()) {
            //FIXME #2290: path with multiple parents not supported
            if (topologyPath[0]->getParents().size() > 1) {
                NES_ERROR("ILPStrategy: Current implementation can not place operators on topology with multiple paths.");
                return false;
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
        LogicalOperatorNodePtr logicalOperatorNode = operatorMap[operatorID]->as<LogicalOperatorNode>();
        if (logicalOperatorNode->getParents().empty()) {
            continue;
        }

        //Loop over downstream operators and compute network cost
        for (auto downStreamOperator : logicalOperatorNode->getParents()) {
            OperatorId downStreamOperatorId = downStreamOperator->as_if<LogicalOperatorNode>()->getId();
            //Only consider nodes that are to be placed
            if (operatorMap.find(downStreamOperatorId) != operatorMap.end()) {

                auto distance = position - operatorPositionMap.find(downStreamOperatorId)->second;
                NES_DEBUG("distance: {} {}", operatorID, distance.to_string());
                double output;
                if (!logicalOperatorNode->hasProperty("output")) {
                    output = getDefaultOperatorOutput(logicalOperatorNode);
                } else {
                    std::any prop = logicalOperatorNode->getProperty("output");
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
        TopologyNodePtr topologyNode = topologyMap[topologyID]->as<TopologyNode>();
        std::any prop = topologyNode->getNodeProperty("slots");
        auto availableSlot = std::any_cast<int>(prop);

        opt.add(currentOverUtilization >= 0);// we only penalize over-utilization, hence its value should be >= 0.
        opt.add(utilization - currentOverUtilization <= availableSlot);// formula for the over-utilization

        overUtilizationCost = overUtilizationCost + currentOverUtilization;
    }

    auto weightOverUtilization = z3Context->real_val(std::to_string(this->overUtilizationCostWeight).c_str());
    auto weightNetwork = z3Context->real_val(std::to_string(this->networkCostWeight).c_str());

    // 5. Optimize ILP problem and print solution.
    opt.minimize(weightNetwork * cost_net + weightOverUtilization * overUtilizationCost);// where the actual optimization happen

    // 6. Check if we have solution, return false if that is not the case
    if (z3::sat != opt.check()) {
        NES_ERROR("ILPStrategy: Solver failed.");
        return false;
    }

    // At this point, we already get the solution.
    // 7. Get the model to retrieve the optimization solution.
    auto z3Model = opt.get_model();
    NES_DEBUG("ILPStrategy:model: {}", z3Model.to_string());
    NES_INFO("Solver found solution with cost: {}", z3Model.eval(cost_net).get_decimal_string(4));

    //get ID of watermark/window
    /*
    int watermarkOpID;
    int windowOpID;
    for (auto operatorNode : this->operatorMap){
        if (operatorNode.second->toString().find("WINDOW") != std::string::npos){
            NES_INFO("window found");
            windowOpID = operatorNode.first;
            NES_INFO("window op id is {}", windowOpID);
        }
        if (operatorNode.second->toString().find("WATERMARK") != std::string::npos){
            NES_INFO("watermark found");
            watermarkOpID = operatorNode.first;
            NES_INFO("waterm op id is {}", watermarkOpID);
        }
    }
    uint64_t watermarkTopologyNode = 0;
    uint64_t windowTopologyNode = 0;
    bool watermarkSet = false;
    bool windowSet = false;*/

    // 7. Pick the solution which has placement decision of 1, i.e., the ILP decide to place the operator in that node
    std::map<OperatorNodePtr, TopologyNodePtr> operatorToTopologyNodeMap;
    for (auto const& [topologyID, P] : placementVariables) {
        if (z3Model.eval(P).get_numeral_int() == 1) {// means we place the operator in the node

            int operatorId = std::stoi(topologyID.substr(0, topologyID.find(",")));
            int topologyNodeId = std::stoi(topologyID.substr(topologyID.find(",") + 1));
            LogicalOperatorNodePtr logicalOperatorNode = operatorMap[operatorId];
            TopologyNodePtr topologyNode = topologyMap[topologyNodeId];
            /*
            if (!((windowSet || watermarkSet) && (watermarkTopologyNode > windowTopologyNode))){
                NES_INFO("windowSet:{} and watermSet: {} and watermarkTNode: {} and windowTNode: {}", windowSet, watermarkSet, windowTopologyNode, watermarkTopologyNode);
                NES_INFO("inside if with {}", operatorId);
                if (watermarkSet) watermarkSet = !watermarkSet;
                if (windowSet) windowSet = !windowSet;*/

            // collect the solution to operatorToTopologyNodeMap
            operatorToTopologyNodeMap.insert(std::make_pair(logicalOperatorNode, topologyNode));
        }
    }

/*
    for (auto elem : operatorToTopologyNodeMap){
        if (int(elem.first->getId()) == watermarkOpID) {
            if (elem.second->getId() <= windowTopologyNode) {
 //               operatorToTopologyNodeMap.insert(std::make_pair(elem.first,));
            }
        }
    }*/

/*
    int windowTopId;
    for (auto const& [topologyID, P] : placementVariables) {
        int operatorId = std::stoi(topologyID.substr(0, topologyID.find(",")));
        int topologyNodeId = std::stoi(topologyID.substr(topologyID.find(",") + 1));
        if (operatorId == windowOpID && topologyNodeId > 2){
            OperatorNodePtr operatorNode = operatorMap[operatorId];
            TopologyNodePtr topologyNode = topologyMap[topologyNodeId];
            operatorToTopologyNodeMap.insert(std::make_pair(operatorNode, topologyNode));
            windowTopId = topologyNodeId;
        }
    }
    for (auto const& [topologyID, P] : placementVariables) {
        int operatorId = std::stoi(topologyID.substr(0, topologyID.find(",")));
        int topologyNodeId = std::stoi(topologyID.substr(topologyID.find(",") + 1));
        if (operatorId == watermarkOpID && topologyNodeId < windowTopId && topologyNodeId > 1){
            OperatorNodePtr operatorNode = operatorMap[operatorId];
            TopologyNodePtr topologyNode = topologyMap[topologyNodeId];
            operatorToTopologyNodeMap.insert(std::make_pair(operatorNode, topologyNode));
        }

    }*/

/*
    int sizeopTNM = operatorToTopologyNodeMap.size();
    int sizeOM = operatorMap.size();
    NES_INFO("size of optm {} and om {}", sizeopTNM, sizeOM);
    */


    NES_INFO("Solver found solution with cost: {}", z3Model.eval(cost_net).get_decimal_string(4));
    for (auto const& [operatorNode, topologyNode] : operatorToTopologyNodeMap) {
        NES_INFO("Operator {} is executed on Topology Node {}", operatorNode->toString(), topologyNode->toString());
    }

    // 8. Pin the operators based on ILP solution.
    pinOperators(z3Model, placementVariables);

    // 8. Perform operator placement.
    placePinnedOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

    // 9. Add network source and sink operators.
    addNetworkSourceAndSinkOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

    // 10. Run the type inference phase and return.
    return runTypeInferencePhase(queryId, faultToleranceType, lineageType);
}

std::map<uint64_t, double> ILPStrategy::computeMileage(const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators) {
    std::map<uint64_t, double> mileageMap;// (topologyId, M)
    // populate the distance map
    for (const auto& pinnedUpStreamOperator : pinnedUpStreamOperators) {
        auto nodeId = std::any_cast<uint64_t>(pinnedUpStreamOperator->getProperty(PINNED_NODE_ID));
        auto topologyNode = topologyMap[nodeId];
        computeDistance(topologyNode, mileageMap);
    }
    return mileageMap;
}

void ILPStrategy::computeDistance(TopologyNodePtr node, std::map<uint64_t, double>& mileages) {
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
    mileages[topologyID] = 1.0 / node->getLinkProperty(parent)->bandwidth + mileages[parentID];
}

void ILPStrategy::addConstraints(z3::optimize& opt,
                                 std::vector<NodePtr>& operatorNodePath,
                                 std::vector<TopologyNodePtr>& topologyNodePath,
                                 std::map<std::string, z3::expr>& placementVariable,
                                 std::map<OperatorId, z3::expr>& operatorDistanceMap,
                                 std::map<uint64_t, z3::expr>& nodeUtilizationMap,
                                 std::map<uint64_t, double>& nodeMileageMap) {

    //get IDs of query operators
    int watermarkOpId;
    int windowOpId;
    int sourceOpId;
    int sinkOpId;
    for (auto operatorNode : operatorNodePath){
        NES_INFO("current operator node is: {}", operatorNode->toString());

        if (operatorNode->toString().find("SINK") != std::string::npos){
            sinkOpId = extractId(operatorNode);
            NES_INFO("operator ID of sink is {}", sinkOpId);
        }
        if (operatorNode->toString().find("WATERMARK") != std::string::npos){
            watermarkOpId = extractId(operatorNode);
            NES_INFO("operator ID of watermark is {}", watermarkOpId);
        }
        if (operatorNode->toString().find("WINDOW") != std::string::npos){
            windowOpId = extractId(operatorNode);
            NES_INFO("operator ID of window is {}", windowOpId);
        }
        if (operatorNode->toString().find("SOURCE") != std::string::npos){
            sourceOpId = extractId(operatorNode);
            NES_INFO("operator ID of source is {}", sourceOpId);
        }
    }

    for (uint64_t i = 0; i < operatorNodePath.size(); i++) {
        auto operatorNode = operatorNodePath[i]->as<LogicalOperatorNode>();
        OperatorId operatorID = operatorNode->getId();

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
        NES_INFO("path size beträgt{}", topologyNodePath.size());
        NES_INFO("IDs for source, sink, waterm, window:  {} {} {} {}", sourceOpId, sinkOpId, watermarkOpId, windowOpId);
        for (uint64_t j = 0; j < topologyNodePath.size(); j++) {
            TopologyNodePtr topologyNode = topologyNodePath[j]->as<TopologyNode>();
            uint64_t topologyID = topologyNode->getId();

            // create placement variable and constraint to {0,1}
            std::string variableID = std::to_string(operatorID) + KEY_SEPARATOR + std::to_string(topologyID);
            auto P_IJ = z3Context->int_const(variableID.c_str());
            if ((i == 0 && j == 0) || (i == operatorNodePath.size() - 1 && j == topologyNodePath.size() - 1)) {
                opt.add(P_IJ == 1);// Fix the placement of source and sink
            } else {
                // The binary decision on whether to place or not, hence we constrain it to be either 0 or 1
                opt.add(P_IJ == 0 || P_IJ == 1);
            }

            NES_INFO("placement of variables with variableID = {} and topologyID = {} and operatorID={} and in {}.loop", variableID, topologyID, operatorID, j+1);
            uint64_t lastIndex = topologyNodePath.size() - 1;
            if (int(operatorID) == sourceOpId && (j == 0)){
                placementVariable.insert(std::make_pair(variableID, P_IJ));
                NES_INFO("in source-if with variableID = {} and topologyD = {} and operatorID={}", variableID, topologyID, operatorID);
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
            if (int(operatorID) == watermarkOpId && (j == 0 || j == 1)){
                placementVariable.insert(std::make_pair(variableID, P_IJ));
                NES_INFO("in watermark-if with variableID = {} and topologyD = {} and operatorID={}", variableID, topologyID, operatorID);
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
            if (int(operatorID) == windowOpId && (j == 1 || j == 2)){
                placementVariable.insert(std::make_pair(variableID, P_IJ));
                NES_INFO("in window-if with variableID = {} and topologyD = {} and operatorID={}", variableID, topologyID, operatorID);
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
            if (int(operatorID) == sinkOpId && j == 2){
                placementVariable.insert(std::make_pair(variableID, P_IJ));
                NES_INFO("in sink-if with variableID = {} and topologyD = {} and operatorID={}", variableID, topologyID, operatorID);
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
            else if (int(operatorID) != windowOpId && int(operatorID) != watermarkOpId && int(operatorID) != sinkOpId && int(operatorID) != sourceOpId){
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

            //placementVariable.insert(std::make_pair(variableID, P_IJ));
            //sum_i = sum_i + P_IJ;
            // add to node utilization
            /*
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
            D_i = D_i + z3Context->real_val(std::to_string(M).c_str()) * P_IJ;*/

        }
        operatorDistanceMap.insert(std::make_pair(operatorID, D_i));
        // add constraint that operator is placed exactly once on topology path
        opt.add(sum_i == 1);
    }
    for (auto elem : placementVariable){
        NES_INFO("placement variables: {}", elem.second.to_string());
    }
}

int ILPStrategy::extractId(const NodePtr& operatorNode) {
    const std::string& str = operatorNode->toString();
    NES_INFO("Inside extractId method the operator Node looks like this: {}", str);
    bool foundOpenParenthesis = false;
    std::string numberString;

    for (char c : str) {
    if (c == '(') {
        if (!foundOpenParenthesis) {
            foundOpenParenthesis = true;
        } else {
            break; // Ignore numbers after the first open parenthesis
        }
    } else if (foundOpenParenthesis && std::isdigit(c)) {
        numberString += c;
        NES_INFO("Inside extractId method the number that was found looks like this: {}", numberString);
    } else if (foundOpenParenthesis && !std::isspace(c)) {
        break; // Stop extracting if a non-space character is encountered after the number
    }
    }
    try {
        NES_INFO("just before return {}", std::stoi(numberString));
        return std::stoi(numberString); // Convert the extracted string to an integer
    } catch (std::exception&) {
    // Handle invalid number extraction, e.g., if no number was found or the number is out of range
        return -1; // Return a sentinel value to indicate failure
    }
}

bool ILPStrategy::pinOperators(z3::model& z3Model, std::map<std::string, z3::expr>& placementVariables) {

    for (const auto& placementMapping : placementVariables) {
        auto key = placementMapping.first;
        uint64_t operatorId = std::stoi(key.substr(0, key.find(KEY_SEPARATOR)));
        uint64_t topologyNodeId = std::stoi(key.substr(key.find(KEY_SEPARATOR) + 1));

        if (z3Model.eval(placementMapping.second).get_numeral_int() == 1) {
            //Pin the operator to the location identified by ILP algorithm
            auto logicalOperator = operatorMap[operatorId];
            logicalOperator->addProperty(PINNED_NODE_ID, topologyNodeId);
        }
    }
    return true;
}

double ILPStrategy::getOverUtilizationCostWeight() { return this->overUtilizationCostWeight; }

double ILPStrategy::getNetworkCostWeight() { return this->networkCostWeight; }

void ILPStrategy::setOverUtilizationWeight(double weight) { this->overUtilizationCostWeight = weight; }

void ILPStrategy::setNetworkCostWeight(double weight) { this->networkCostWeight = weight; }

//FIXME: in #1422. This need to be defined better as at present irrespective of operator location we are returning always the default value
double ILPStrategy::getDefaultOperatorOutput(LogicalOperatorNodePtr operatorNode) {

    double dmf = 1;
    double input = 10;
    if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        dmf = 0;
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        dmf = .5;
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        dmf = 2;
    } else if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        input = 100;
    }
    return dmf * input;
}

//FIXME: in #1422. This need to be defined better as at present irrespective of operator location we are returning always the default value
int ILPStrategy::getDefaultOperatorCost(LogicalOperatorNodePtr operatorNode) {

    if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        return 0;
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        return 1;
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>() || operatorNode->instanceOf<JoinLogicalOperatorNode>()
               || operatorNode->instanceOf<UnionLogicalOperatorNode>()) {
        return 2;
    } else if (operatorNode->instanceOf<ProjectionLogicalOperatorNode>()) {
        return 1;
    } else if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        return 0;
    }
    return 2;
}

}// namespace NES::Optimizer