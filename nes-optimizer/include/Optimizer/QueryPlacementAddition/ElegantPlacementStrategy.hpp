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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_ELEGANTPLACEMENTSTRATEGY_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_ELEGANTPLACEMENTSTRATEGY_HPP_

#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <cpr/response.h>
#include <nlohmann/json.hpp>

namespace NES::Optimizer {

/**
 * @brief This class allows us to communicate with external planner service to perform operator placement.
 */
class ElegantPlacementStrategy : public BasePlacementAdditionStrategy {
  public:
    // Keys for information that is used during ELEGANT placement and stored in node properties.
    // This key is also accessed in SampleCodeGenerationPhase.cpp.
    const static std::string sourceCodeKey;

  public:
    ~ElegantPlacementStrategy() override = default;

    /**
     * @brief Create instance of Elegant Placement Strategy
     * @param serviceURL: service URL to connect to
     * @param placementStrategy: The name of the ELEGANT placement strategy
     * @param globalExecutionPlan: the global execution plan
     * @param topology: the topology
     * @param typeInferencePhase: type inference phase
     * @param placementMode: placement mode
     * @return shared pointer to the placement strategy
     */
    static BasePlacementStrategyPtr create(const std::string& serviceURL,
                                           PlacementStrategy placementStrategy,
                                           const GlobalExecutionPlanPtr& globalExecutionPlan,
                                           const TopologyPtr& topology,
                                           const TypeInferencePhasePtr& typeInferencePhase,
                                           PlacementAmendmentMode placementAmendmentMode);

    PlacementAdditionResult updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                                      const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                                      const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                                      DecomposedQueryPlanVersion querySubPlanVersion, FaultToleranceType faultToleranceType = FaultToleranceType::NONE) override;

  private:
    explicit ElegantPlacementStrategy(const std::string& serviceURL,
                                      const float timeWeight,
                                      const GlobalExecutionPlanPtr& globalExecutionPlan,
                                      const TopologyPtr& topology,
                                      const TypeInferencePhasePtr& typeInferencePhase,
                                      PlacementAmendmentMode placementAmendmentMode);

    /**
     * @brief: Prepare the query payload for the external service
     * @param pinnedUpStreamOperators: pinned upstream operators of the plan to be placed
     * @param pinnedDownStreamOperators: pinned downstream operators of the plan to be placed
     * @return json representing the payload
     */
    void prepareQueryPayload(const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                             const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                             nlohmann::json&);

    /**
     * @brief: Prepare the topology payload for the external placement service
     * @return json representing the payload
     */
    void prepareTopologyPayload(nlohmann::json&);

    /**
     * @brief Add pinning information to the operators based on the response received from the external placement service
     * @param sharedQueryId: shared query id
     * @param pinnedDownStreamOperators: pinned downstream operators
     * @param response: Json representing the placement response received from the external service
     */
    void pinOperatorsBasedOnElegantService(SharedQueryId sharedQueryId,
                                           const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                           cpr::Response& response) const;

    /**
     * @brief Add a base64-transformed Java bytecode list to the JSON representation of the operator, if the operator is a MapUDFLogicalOperator or FlatMapUDFLogicalOperator. Otherwise, add an empty field.
     * @param logicalOperator The logical operator that is processed.
     * @param node Target JSON operator.
     */
    void addJavaUdfByteCodeField(const OperatorPtr& logicalOperator, nlohmann::json& node);

    std::string serviceURL;
    float timeWeight;

    const int32_t ELEGANT_SERVICE_TIMEOUT = 3000;
    const std::string EMPTY_STRING;
};

}// namespace NES::Optimizer

#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_ELEGANTPLACEMENTSTRATEGY_HPP_
