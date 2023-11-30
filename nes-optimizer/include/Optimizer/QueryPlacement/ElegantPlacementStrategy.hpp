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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENT_ELEGANTPLACEMENTSTRATEGY_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENT_ELEGANTPLACEMENTSTRATEGY_HPP_

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Util/PlacementStrategy.hpp>
#include <cpr/response.h>
#include <nlohmann/json.hpp>

namespace NES::Optimizer {

/**
 * @brief This class allows us to communicate with external planner service to perform operator placement.
 */
class ElegantPlacementStrategy : public BasePlacementStrategy {
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
     * @return shared pointer to the placement strategy
     */
    static std::unique_ptr<ElegantPlacementStrategy> create(const std::string& serviceURL,
                                                            const float transferRate,
                                                            PlacementStrategy placementStrategy,
                                                            GlobalExecutionPlanPtr globalExecutionPlan,
                                                            TopologyPtr topology,
                                                            TypeInferencePhasePtr typeInferencePhase);

    bool updateGlobalExecutionPlan(QueryId queryId,
                                   const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                   const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) override;

  private:
    explicit ElegantPlacementStrategy(const std::string& serviceURL,
                                      const float transferRate,
                                      const float timeWeight,
                                      GlobalExecutionPlanPtr globalExecutionPlan,
                                      TopologyPtr topology,
                                      TypeInferencePhasePtr typeInferencePhase);

    /**
     * @brief: Prepare the query payload for the external service
     * @param pinnedUpStreamOperators: pinned upstream operators of the plan to be placed
     * @param pinnedDownStreamOperators: pinned downstream operators of the plan to be placed
     * @return json representing the payload
     */
    void prepareQueryPayload(const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                       const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators,
                                       nlohmann::json&);

    /**
     * @brief: Prepare the topology payload for the external placement service
     * @return json representing the payload
     */
    void prepareTopologyPayload(nlohmann::json&);

    /**
     * @brief Add pinning information to the operators based on the response received from the external placement service
     * @param queryId: query id
     * @param pinnedDownStreamOperators: pinned downstream operators
     * @param response: Json representing the placement response received from the external service
     */
    void pinOperatorsBasedOnElegantService(QueryId queryId,
                                           const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators,
                                           cpr::Response& response) const;

    /**
     * @brief Add a base64-transformed Java bytecode list to the JSON representation of the operator, if the operator is a MapUDFLogicalOperatorNode or FlatMapUDFLogicalOperatorNode. Otherwise, add an empty field.
     * @param logicalOperator The logical operator that is processed.
     * @param node Target JSON operator.
     */
    void addJavaUdfByteCodeField(const OperatorNodePtr& logicalOperator, nlohmann::json& node);

    std::string serviceURL;
    float transferRate;
    float timeWeight;

    const int32_t ELEGANT_SERVICE_TIMEOUT = 3000;

    //Query payload constants
    const std::string OPERATOR_GRAPH_KEY = "operatorGraph";
    const std::string OPERATOR_ID_KEY = "operatorId";
    const std::string CHILDREN_KEY = "children";
    const std::string CONSTRAINT_KEY = "constraint";
    const std::string INPUT_DATA_KEY = "inputData";
    const std::string JAVA_UDF_FIELD_KEY = "javaUdfField";

    //Topology payload constants
    const std::string DEVICE_ID_KEY = "deviceID";
    const std::string DEVICES_KEY = "devices";
    const std::string NODE_ID_KEY = "nodeId";
    const std::string NODE_TYPE_KEY = "nodeType";

    //Network delay payload constants
    const std::string NETWORK_DELAYS_KEY = "networkDelays";
    const std::string LINK_ID_KEY = "linkID";
    const std::string TRANSFER_RATE_KEY = "transferRate";
    const std::string TIME_WEIGHT_KEY = "time_weight";

    //Response payload constants
    const std::string AVAILABLE_NODES_KEY = "availNodes";
    const std::string PLACEMENT_KEY = "placement";

    const std::string EMPTY_STRING;
};

}// namespace NES::Optimizer

#endif  // NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENT_ELEGANTPLACEMENTSTRATEGY_HPP_
