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
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Util/Placement/ElegantPayloadKeys.hpp>
#include <Operators/LogicalOperators/OpenCLLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UDFs/FlatMapUDF/FlatMapUDFLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Operators/LogicalOperators/UDFs/MapUDF/MapUDFLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/Exceptions/QueryPlacementException.hpp>
#include <Optimizer/QueryPlacementAddition/ElegantPlacementStrategy.hpp>
#include <Runtime/OpenCLDeviceInfo.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <cpp-base64/base64.h>
#include <cpr/api.h>
#include <queue>
#include <utility>

namespace NES::Optimizer {

const std::string ElegantPlacementStrategy::sourceCodeKey = "sourceCode";

BasePlacementStrategyPtr ElegantPlacementStrategy::create(const std::string& serviceURL,
                                                          PlacementStrategy placementStrategy,
                                                          const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                          const TopologyPtr& topology,
                                                          const TypeInferencePhasePtr& typeInferencePhase,
                                                          PlacementAmenderMode placementAmenderMode) {

    float timeWeight = 0.0;

    switch (placementStrategy) {
        case PlacementStrategy::ELEGANT_PERFORMANCE: timeWeight = 1; break;
        case PlacementStrategy::ELEGANT_ENERGY: timeWeight = 0; break;
        case PlacementStrategy::ELEGANT_BALANCED: timeWeight = 0.5; break;
        default: NES_ERROR("Unknown placement strategy for elegant {}", magic_enum::enum_name(placementStrategy));
    }

    return std::make_unique<ElegantPlacementStrategy>(ElegantPlacementStrategy(serviceURL,
                                                                               timeWeight,
                                                                               globalExecutionPlan,
                                                                               topology,
                                                                               typeInferencePhase,
                                                                               placementAmenderMode));
}

ElegantPlacementStrategy::ElegantPlacementStrategy(const std::string& serviceURL,
                                                   const float timeWeight,
                                                   const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                   const TopologyPtr& topology,
                                                   const TypeInferencePhasePtr& typeInferencePhase,
                                                   PlacementAmenderMode placementAmenderMode)
    : BasePlacementAdditionStrategy(globalExecutionPlan, topology, typeInferencePhase, placementAmenderMode), serviceURL(serviceURL),
      timeWeight(timeWeight) {}

bool ElegantPlacementStrategy::updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                                         const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                                         const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) {

    try {
        NES_ASSERT(serviceURL != EMPTY_STRING, "ELEGANT planner URL is not set in elegant.plannerServiceURL");
        nlohmann::json payload{};
        prepareQueryPayload(pinnedUpStreamOperators, pinnedDownStreamOperators, payload);
        prepareTopologyPayload(payload);
        payload[TIME_WEIGHT_KEY] = timeWeight;
        NES_INFO("Sending placement request to ELEGANT planner with payload: url={}, payload={}", serviceURL, payload.dump());
        cpr::Response response = cpr::Post(cpr::Url{serviceURL},
                                           cpr::Header{{"Content-Type", "application/json"}},
                                           cpr::Body{payload.dump()},
                                           cpr::Timeout(ELEGANT_SERVICE_TIMEOUT));
        if (response.status_code != 200) {
            throw Exceptions::QueryPlacementException(
                sharedQueryId,
                "ElegantPlacementStrategy::updateGlobalExecutionPlan: Error in call to Elegant planner with code "
                    + std::to_string(response.status_code) + " and msg " + response.reason);
        }

        // 2. Create copy of the query plan
        auto copiedPinnedOperators = createCopyOfQueryPlan(pinnedUpStreamOperators, pinnedDownStreamOperators);

        // 3. Parse the response of the external placement service
        pinOperatorsBasedOnElegantService(sharedQueryId, copiedPinnedOperators.copiedPinnedDownStreamOperators, response);

        // 4. Compute query sub plans
        auto computedQuerySubPlans = computeQuerySubPlans(sharedQueryId,
                                                          copiedPinnedOperators.copiedPinnedUpStreamOperators,
                                                          copiedPinnedOperators.copiedPinnedDownStreamOperators);

        // 5. add network source and sink operators
        addNetworkOperators(computedQuerySubPlans);

        // 6. update execution nodes
        return updateExecutionNodes(sharedQueryId, computedQuerySubPlans);
    } catch (const std::exception& ex) {
        throw Exceptions::QueryPlacementException(sharedQueryId, ex.what());
    }
}

void ElegantPlacementStrategy::pinOperatorsBasedOnElegantService(
    SharedQueryId sharedQueryId,
    const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators,
    cpr::Response& response) const {
    nlohmann::json jsonResponse = nlohmann::json::parse(response.text);
    //Fetch the placement data
    auto placementData = jsonResponse[PLACEMENT_KEY];

    // fill with true where nodeId is present
    for (const auto& placement : placementData) {
        OperatorId operatorId = placement[OPERATOR_ID_KEY];
        WorkerId topologyNodeId = placement[NODE_ID_KEY];

        bool pinned = false;
        for (const auto& item : pinnedDownStreamOperators) {
            auto operatorToPin = item->getChildWithOperatorId(operatorId)->as<OperatorNode>();
            if (operatorToPin) {
                operatorToPin->addProperty(PINNED_WORKER_ID, topologyNodeId);

                if (operatorToPin->instanceOf<OpenCLLogicalOperatorNode>()) {
                    size_t deviceId = placement[DEVICE_ID_KEY];
                    operatorToPin->as<OpenCLLogicalOperatorNode>()->setDeviceId(deviceId);
                }

                pinned = true;
                break;
            }
        }

        if (!pinned) {
            throw Exceptions::QueryPlacementException(sharedQueryId,
                                                      "Unable to find operator with id " + std::to_string(operatorId)
                                                          + " in the given list of operators.");
        }
    }
}

void ElegantPlacementStrategy::addJavaUdfByteCodeField(const OperatorNodePtr& logicalOperator, nlohmann::json& node) {
    if (logicalOperator->instanceOf<MapUDFLogicalOperatorNode>()
        || logicalOperator->instanceOf<FlatMapUDFLogicalOperatorNode>()) {
        const auto* udfDescriptor =
            dynamic_cast<Catalogs::UDF::JavaUDFDescriptor*>(logicalOperator->as<UDFLogicalOperator>()->getUDFDescriptor().get());
        const auto& byteCode = udfDescriptor->getByteCodeList();
        std::vector<std::pair<std::string, std::string>> base64ByteCodeList;
        std::transform(byteCode.cbegin(),
                       byteCode.cend(),
                       std::back_inserter(base64ByteCodeList),
                       [](const jni::JavaClassDefinition& classDefinition) {
                           return std::pair<std::string, std::string>{
                               classDefinition.first,
                               base64_encode(std::string(classDefinition.second.data(), classDefinition.second.size()))};
                       });
        node[JAVA_UDF_FIELD_KEY] = base64ByteCodeList;
    } else {
        node[JAVA_UDF_FIELD_KEY] = "";
    }
}

void ElegantPlacementStrategy::prepareQueryPayload(const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                                   const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators,
                                                   nlohmann::json& payload) {

    NES_DEBUG("Getting the json representation of the query plan");

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
            node[OPERATOR_ID_KEY] = logicalOperator->getId();
            auto pinnedNodeId = logicalOperator->getProperty(PINNED_WORKER_ID);
            node[CONSTRAINT_KEY] =
                pinnedNodeId.has_value() ? std::to_string(std::any_cast<WorkerId>(pinnedNodeId)) : EMPTY_STRING;
            auto sourceCode = logicalOperator->getProperty(sourceCodeKey);
            node[sourceCodeKey] = sourceCode.has_value() ? std::any_cast<std::string>(sourceCode) : EMPTY_STRING;
            node[INPUT_DATA_KEY] = logicalOperator->getOutputSchema()->getSchemaSizeInBytes();
            addJavaUdfByteCodeField(logicalOperator, node);

            auto found = std::find_if(pinnedUpStreamOperators.begin(),
                                      pinnedUpStreamOperators.end(),
                                      [logicalOperator](const OperatorNodePtr& pinnedOperator) {
                                          return pinnedOperator->getId() == logicalOperator->getId();
                                      });

            //array of upstream operator ids
            auto upstreamOperatorIds = nlohmann::json::array();
            //Only explore further upstream operators if this operator is not in the list of pinned upstream operators
            if (found == pinnedUpStreamOperators.end()) {
                for (const auto& upstreamOperator : logicalOperator->getChildren()) {
                    upstreamOperatorIds.push_back(upstreamOperator->as<OperatorNode>()->getId());
                    operatorsToVisit.emplace(upstreamOperator->as<OperatorNode>());// add children for future visit
                }
            }
            node[CHILDREN_KEY] = upstreamOperatorIds;
            nodes.push_back(node);
        }
    }
    payload[OPERATOR_GRAPH_KEY] = nodes;
}

void ElegantPlacementStrategy::prepareTopologyPayload(nlohmann::json& payload) { topology->getElegantPayload(payload); }

}// namespace NES::Optimizer