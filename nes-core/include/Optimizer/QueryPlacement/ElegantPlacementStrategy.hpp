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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_QUERYPLACEMENT_EXTERNALICCSPLACEMENTSTRATEGY_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_QUERYPLACEMENT_EXTERNALICCSPLACEMENTSTRATEGY_HPP_

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Util/PlacementStrategy.hpp>
#include <cpr/response.h>
#include <nlohmann/json.hpp>

namespace NES::Optimizer {

class ElegantPlacementStrategy : public BasePlacementStrategy {
  public:
    ~ElegantPlacementStrategy() override = default;

    /**
     * @brief
     * @param serviceURL
     * @param placementStrategy
     * @param globalExecutionPlan
     * @param topology
     * @param typeInferencePhase
     * @return
     */
    static std::unique_ptr<ElegantPlacementStrategy> create(const std::string& serviceURL,
                                                            PlacementStrategy placementStrategy,
                                                            GlobalExecutionPlanPtr globalExecutionPlan,
                                                            TopologyPtr topology,
                                                            TypeInferencePhasePtr typeInferencePhase);

    bool updateGlobalExecutionPlan(QueryId queryId,
                                   FaultToleranceType faultToleranceType,
                                   LineageType lineageType,
                                   const std::set<OperatorNodePtr>& pinnedUpStreamOperators,
                                   const std::set<OperatorNodePtr>& pinnedDownStreamOperators) override;

  private:
    explicit ElegantPlacementStrategy(std::string serviceURL,
                                      uint16_t performanceRatio,
                                      uint16_t energyRatio,
                                      GlobalExecutionPlanPtr globalExecutionPlan,
                                      TopologyPtr topology,
                                      TypeInferencePhasePtr typeInferencePhase);

    /**
     *
     * @param pinnedUpStreamOperators
     * @param pinnedDownStreamOperators
     * @return
     */
    nlohmann::json prepareQueryPayload(const std::set<OperatorNodePtr>& pinnedUpStreamOperators,
                                       const std::set<OperatorNodePtr>& pinnedDownStreamOperators);

    /**
     * @brief
     * @return
     */
    nlohmann::json prepareTopologyPayload();

    /**
     * @brief
     * @param queryId
     * @param pinnedDownStreamOperators
     * @param response
     */
    void pinOperatorsBasedOnElegantService(QueryId queryId,
                                           const std::set<OperatorNodePtr>& pinnedDownStreamOperators,
                                           cpr::Response& response) const;

    std::string serviceURL;
    uint16_t performanceRatio;
    uint16_t energyRatio;

    //Payload constants
    const std::string OPERATOR_GRAPH = "operatorGraph";
    const std::string CHILDREN = "children";
    const std::string OUTPUT_TUPLE_SIZE = "outputTupleSize";
    const std::string SOURCE_CODE = "sourceCode";
    const std::string PINNED_NODE_ID = "pinnedNodeId";
    const std::string ID = "id";
    const std::string DEVICE_ID = "deviceID";
    const std::string DEVICE_TYPE = "deviceType";
    const std::string DEVICE_NAME = "deviceName";
    const std::string MEMORY = "memory";
    const std::string DEVICES = "devices";
    const std::string NODE_TYPE = "nodeType";
    const std::string LINK_ID = "linkID";
    const std::string SOURCE = "source";
    const std::string TARGET = "target";
    const std::string TRANSFER_RATE = "transferRate";
    const std::string AVAILABLE_NODES = "AVAILABLE_NODES";
    const std::string NETWORK_DELAYS = "NETWORK_DELAYS";
    const std::string NODE_ID = "nodeId";
    const std::string PLACEMENT = "placement";
    const std::string OPERATOR_ID = "operatorId";
    const std::string ENERGY_RATIO = "energyRatio";
    const std::string PERFORMANCE_RATIO = "performanceRatio";
    const std::string OPTIMIZATION_OBJECTIVES = "optimizationObjectives";
};

}// namespace NES::Optimizer

#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYPLACEMENT_EXTERNALICCSPLACEMENTSTRATEGY_HPP_
