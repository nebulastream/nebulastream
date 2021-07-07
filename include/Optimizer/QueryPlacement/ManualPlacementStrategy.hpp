/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_MANUALPLACEMENTSTRATEGY_HPP
#define NES_MANUALPLACEMENTSTRATEGY_HPP

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>

namespace NES::Optimizer {

/**
 * @brief A placement strategy where users specify manually where to place operators in the topology
 */
class ManualPlacementStrategy : public BasePlacementStrategy {
  public:
    ~ManualPlacementStrategy() override = default;

    bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan) override;

    bool partiallyUpdateGlobalExecutionPlan(const QueryPlanPtr& queryPlan) override;

    static std::unique_ptr<ManualPlacementStrategy> create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                           TopologyPtr topology,
                                                           TypeInferencePhasePtr typeInferencePhase,
                                                           StreamCatalogPtr streamCatalog);

    /**
     * @brief set the binary mapping of the current strategy
     * a binary mapping is a 2D vector of i X j that store decision whether to place operator j in node i
     * @param userDefinedBinaryMapping binary mapping to set
     */
    void setBinaryMapping(std::vector<std::vector<bool>> userDefinedBinaryMapping);

  private:
    explicit ManualPlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                     TopologyPtr topology,
                                     TypeInferencePhasePtr typeInferencePhase,
                                     StreamCatalogPtr streamCatalog);

    // stores the binary mapping  of the current strategy
    std::vector<std::vector<bool>> binaryMapping;
};

}// namespace NES::Optimizer

#endif//NES_MANUALPLACEMENTSTRATEGY_HPP
