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

#ifndef NES_RANDOMSEARCHSTRATEGY_HPP
#define NES_RANDOMSEARCHSTRATEGY_HPP

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>

namespace NES::Optimizer {

class RandomSearchStrategy : public BasePlacementStrategy {
  public:
    ~RandomSearchStrategy() override = default;

    bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan) override;

    static std::unique_ptr<RandomSearchStrategy> create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                        TopologyPtr topology,
                                                        TypeInferencePhasePtr typeInferencePhase,
                                                        StreamCatalogPtr streamCatalog);

  private:
    explicit RandomSearchStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                              TopologyPtr topology,
                              TypeInferencePhasePtr typeInferencePhase,
                              StreamCatalogPtr streamCatalog);
};

}// namespace NES::Optimizer

#endif//NES_RANDOMSEARCHSTRATEGY_HPP
