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

#include <API/Schema.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/LogicalOperators/OpenCLLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/QueryPlacement/ElegantPlacementStrategy.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <cpr/api.h>
#include <queue>
#include <utility>

namespace NES::Optimizer {

std::unique_ptr<ElegantPlacementStrategy>
ElegantPlacementStrategy::create(const std::string& serviceURL,
                                 PlacementStrategy placementStrategy,
                                 NES::GlobalExecutionPlanPtr globalExecutionPlan,
                                 NES::TopologyPtr topology,
                                 NES::Optimizer::TypeInferencePhasePtr typeInferencePhase) {

    uint16_t performanceRatio = 0, energyRatio = 0;

    switch (placementStrategy) {
        case PlacementStrategy::ELEGANT_PERFORMANCE:
            performanceRatio = 100;
            energyRatio = 0;
            break;
        case PlacementStrategy::ELEGANT_ENERGY:
            performanceRatio = 0;
            energyRatio = 100;
            break;
        case PlacementStrategy::ELEGANT_BALANCED:
            performanceRatio = 50;
            energyRatio = 50;
            break;
        default: NES_ERROR2("Unknown placement strategy for elegant {}", magic_enum::enum_name(placementStrategy));
    }

    return std::make_unique<ElegantPlacementStrategy>(ElegantPlacementStrategy(serviceURL,
                                                                               performanceRatio,
                                                                               energyRatio,
                                                                               std::move(globalExecutionPlan),
                                                                               std::move(topology),
                                                                               std::move(typeInferencePhase)));
}

ElegantPlacementStrategy::ElegantPlacementStrategy(std::string serviceURL,
                                                   uint16_t performanceRatio,
                                                   uint16_t energyRatio,
                                                   NES::GlobalExecutionPlanPtr globalExecutionPlan,
                                                   NES::TopologyPtr topology,
                                                   NES::Optimizer::TypeInferencePhasePtr typeInferencePhase)
    : BasePlacementStrategy(std::move(globalExecutionPlan), std::move(topology), std::move(typeInferencePhase)),
      serviceURL(std::move(serviceURL)), performanceRatio(performanceRatio), energyRatio(energyRatio) {}

bool ElegantPlacementStrategy::updateGlobalExecutionPlan(
    QueryId queryId /*queryId*/,
    FaultToleranceType faultToleranceType /*faultToleranceType*/,
    LineageType lineageType /*lineageType*/,
    const std::set<OperatorNodePtr>& pinnedUpStreamOperators /*pinnedUpStreamNodes*/,
    const std::set<OperatorNodePtr>& pinnedDownStreamOperators /*pinnedDownStreamNodes*/) {

    try {

        //1. Make rest call to get the external operator placement service.
        //prepare request payload
        nlohmann::json payload{};

        //1.a: Get query plan as json
        auto queryGraph = prepareQueryPayload(pinnedDownStreamOperators, pinnedUpStreamOperators);
        payload.push_back(queryGraph);

        // 1.b: Get topology information as json
        auto availableNodes = prepareTopologyPayload();
        payload.push_back(availableNodes);

        nlohmann::json optimizationObjectives;
        optimizationObjectives["energyRatio"] = energyRatio;
        optimizationObjectives["performanceRatio"] = performanceRatio;
        payload["optimizationObjectives"] = optimizationObjectives;

        //1.c: Make a rest call to elegant planner
        cpr::Response response = cpr::Post(cpr::Url{serviceURL},
                                           cpr::Header{{"Content-Type", "application/json"}},
                                           cpr::Body{payload.dump()},
                                           cpr::Timeout(3000));
        if (response.status_code != 200) {
            NES_ERROR2(
                "ElegantPlacementStrategy::updateGlobalExecutionPlan: Error in call to Elegant planner with code {} and msg {}",
                response.status_code,
                response.reason);
            throw QueryPlacementException(
                queryId,
                "ElegantPlacementStrategy::updateGlobalExecutionPlan: Error in call to Elegant planner with code "
                    + std::to_string(response.status_code) + " and msg " + response.reason);
        }

        //2. Parse the response of the external placement service
        pinOperatorsBasedOnElegantService(queryId, pinnedDownStreamOperators, response);

        // 3. Place the operators
        placePinnedOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 4. add network source and sink operators
        addNetworkSourceAndSinkOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 5. Perform type inference on updated query plans
        return runTypeInferencePhase(queryId, faultToleranceType, lineageType);
    } catch (const std::exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
}

void ElegantPlacementStrategy::pinOperatorsBasedOnElegantService(QueryId queryId,
                                                                 const std::set<OperatorNodePtr>& pinnedDownStreamOperators,
                                                                 cpr::Response& response) const {
    nlohmann::json jsonResponse = nlohmann::json::parse(response.text);
    //Fetch the placement data
    auto placementData = jsonResponse["placement"];

    // fill with true where nodeId is present
    for (const auto& placement : placementData) {
        OperatorId operatorId = placement["operatorId"];
        TopologyNodeId topologyNodeId = placement["nodeId"];

        bool pinned = false;
        for (const auto& item : pinnedDownStreamOperators) {
            auto operatorToPin = item->getChildWithOperatorId(operatorId)->as<OperatorNode>();
            if (operatorToPin) {
                operatorToPin->addProperty(PINNED_NODE_ID, topologyNodeId);

                if (operatorToPin->instanceOf<OpenCLLogicalOperatorNode>()) {
                    std::string deviceId = placement["deviceId"];
                    operatorToPin->as<OpenCLLogicalOperatorNode>()->setDeviceId(deviceId);
                }

                pinned = true;
                break;
            }
        }

        if (!pinned) {
            NES_ERROR2("ElegantPlacementStrategy: Unable to find operator with id {} in the given list of operators.",
                       operatorId);
            throw QueryPlacementException(queryId,
                                          "ElegantPlacementStrategy: Unable to find operator with id "
                                              + std::to_string(operatorId) + " in the given list of operators.");
        }
    }
}

nlohmann::json ElegantPlacementStrategy::prepareQueryPayload(const std::set<OperatorNodePtr>& pinnedUpStreamOperators,
                                                             const std::set<OperatorNodePtr>& pinnedDownStreamOperators) {

    NES_DEBUG2("ElegantPlacementStrategy: Getting the json representation of the query plan");

    nlohmann::json queryPlan{};
    std::vector<nlohmann::json> nodes{};

    std::set<OperatorNodePtr> visitedOperator;
    std::queue<OperatorNodePtr> operatorsToVisit;
    //initialize with upstream operators
    for (const auto& pinnedDownStreamOperator : pinnedDownStreamOperators) {
        operatorsToVisit.emplace(pinnedDownStreamOperator);
    }

    while (!operatorsToVisit.empty()) {

        auto logicalOperator = operatorsToVisit.front();//fetch the front operator
        operatorsToVisit.pop();                         //pop the front operator

        //if operator was not previously visited
        if (visitedOperator.insert(logicalOperator).second) {
            nlohmann::json node;
            node["id"] = logicalOperator->getId();
            auto pinnedNodeId = logicalOperator->getProperty(PINNED_NODE_ID);
            node["PINNED_NODE_ID"] = pinnedNodeId.has_value() ? std::any_cast<uint64_t>(pinnedNodeId) : INVALID_TOPOLOGY_NODE_ID;
            auto sourceCode = logicalOperator->getProperty(SOURCE_CODE);
            node["SOURCE_CODE"] = sourceCode.has_value() ? std::any_cast<std::string>(sourceCode) : "";
            nodes.push_back(node);
            node["OUTPUT_TUPLE_SIZE"] = logicalOperator->getOutputSchema()->getSchemaSizeInBytes();

            auto found = std::find_if(pinnedUpStreamOperators.begin(),
                                      pinnedUpStreamOperators.end(),
                                      [](const OperatorNodePtr& pinnedOperatorNode) {
                                          return pinnedOperatorNode->getId() == 1;
                                      });

            //array of upstream operator ids
            auto upstreamOperatorIds = nlohmann::json::array();
            //Only explore further upstream operators if this operator is not in the list of pinned upstream operators
            if (found != pinnedUpStreamOperators.end()) {
                for (const auto& upstreamOperator : logicalOperator->getChildren()) {
                    upstreamOperatorIds.push_back(upstreamOperator->as<OperatorNode>()->getId());
                    operatorsToVisit.emplace(upstreamOperator->as<OperatorNode>());// add children for future visit
                }
            }
            node["CHILDREN"] = upstreamOperatorIds;
        }
    }
    queryPlan["OPERATOR_GRAPH"] = nodes;
    return queryPlan;
}

nlohmann::json ElegantPlacementStrategy::prepareTopologyPayload() {
    NES_DEBUG2("ElegantPlacementStrategy: Getting the json representation of available nodes");
    nlohmann::json topologyJson{};
    auto root = topology->getRoot();
    std::deque<TopologyNodePtr> parentToAdd{std::move(root)};
    std::deque<TopologyNodePtr> childToAdd;

    std::vector<nlohmann::json> nodes = {};
    std::vector<nlohmann::json> edges = {};

    while (!parentToAdd.empty()) {
        // Current topology node to add to the JSON
        TopologyNodePtr currentNode = parentToAdd.front();
        nlohmann::json currentNodeJsonValue{};
        std::vector<nlohmann::json> devices = {};
        nlohmann::json currentNodeOpenCLJsonValue{};
        parentToAdd.pop_front();

        // Add properties for current topology node
        currentNodeJsonValue["nodeId"] = currentNode->getId();
        currentNodeOpenCLJsonValue["deviceID"] = std::any_cast<std::string>(currentNode->getNodeProperty("DEVICE_ID"));
        currentNodeOpenCLJsonValue["deviceType"] = std::any_cast<std::string>(currentNode->getNodeProperty("DEVICE_TYPE"));
        currentNodeOpenCLJsonValue["deviceName"] = std::any_cast<std::string>(currentNode->getNodeProperty("DEVICE_NAME"));
        currentNodeOpenCLJsonValue["memory"] = std::any_cast<uint64_t>(currentNode->getNodeProperty("DEVICE_MEMORY"));
        devices.push_back(currentNodeOpenCLJsonValue);
        currentNodeJsonValue["devices"] = devices;
        currentNodeJsonValue["nodeType"] = "static";// FIXME: populate from enum of {mobile, static, w/e else}?
        auto children = currentNode->getChildren();
        for (const auto& child : children) {
            // Add edge information for current topology node
            nlohmann::json currentEdgeJsonValue{};
            currentEdgeJsonValue["linkID"] =
                std::to_string(child->as<TopologyNode>()->getId()) + "_" + std::to_string(currentNode->getId());
            currentEdgeJsonValue["source"] = child->as<TopologyNode>()->getId();
            currentEdgeJsonValue["target"] = currentNode->getId();
            currentEdgeJsonValue["transferRate"] = 100;// FIXME: replace it with more intelligible value
            edges.push_back(currentEdgeJsonValue);
            childToAdd.push_back(child->as<TopologyNode>());
        }

        if (parentToAdd.empty()) {
            parentToAdd.insert(parentToAdd.end(), childToAdd.begin(), childToAdd.end());
            childToAdd.clear();
        }

        nodes.push_back(currentNodeJsonValue);
    }
    NES_INFO2("ElegantPlacementStrategy: no more topology nodes to add");

    topologyJson["AVAILABLE_NODES"] = nodes;
    topologyJson["NETWORK_DELAYS"] = edges;
    return topologyJson;
}

}// namespace NES::Optimizer