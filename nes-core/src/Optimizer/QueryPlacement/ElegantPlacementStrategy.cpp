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

#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/LogicalOperators/OpenCLLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/QueryPlacement/ElegantPlacementStrategy.hpp>
#include <Topology/Topology.hpp>
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
    const std::vector<OperatorNodePtr>& pinnedUpStreamOperators /*pinnedUpStreamNodes*/,
    const std::vector<OperatorNodePtr>& pinnedDownStreamOperators /*pinnedDownStreamNodes*/) {

    try {

        //1. Make rest call to get the external operator placement service.

        //prepare request payload
        nlohmann::json payload{};

        //1.a: Get query plan as json
        auto queryGraph = prepareQueryPayload(pinnedDownStreamOperators, pinnedUpStreamOperators);
        payload.push_back(queryGraph);

        //TODO: Write code to properly call the service URL
        // Convert topology to a json. Look at TopologyManagerService:getTopologyAsJson(); Add network related information as well

        //1.c: Make a rest call to elegant planner
        cpr::Response response = cpr::Post(cpr::Url{serviceURL},
                                           cpr::Header{{"Content-Type", "application/json"}},
                                           cpr::Body{payload.dump()},
                                           cpr::Timeout(3000));

        //2. Parse the response of the external placement service
        pinOperatorsBasedOnElegantService(queryId, pinnedDownStreamOperators, response);

        // 3. Place the operators
        placePinnedOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 4. add network source and sink operators
        addNetworkSourceAndSinkOperators(queryId, pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 5. Perform type inference on aUtilityFunctionsll updated query plans
        return runTypeInferencePhase(queryId, faultToleranceType, lineageType);
    } catch (const std::exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
}

void ElegantPlacementStrategy::pinOperatorsBasedOnElegantService(QueryId queryId,
                                                                 const std::vector<OperatorNodePtr>& pinnedDownStreamOperators,
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

nlohmann::json ElegantPlacementStrategy::prepareQueryPayload(const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                                             const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) {

    NES_DEBUG2("ElegantPlacementStrategy: Getting the json representation of the query plan");

    nlohmann::json queryPlan{};
    std::vector<nlohmann::json> nodes{};

    std::set<OperatorNodePtr> visitedOperator;
    std::queue<OperatorNodePtr> operatorsToVisit;
    operatorsToVisit.emplace(pinnedDownStreamOperators.begin(), pinnedDownStreamOperators.end());

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

}// namespace NES::Optimizer