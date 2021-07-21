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

#ifndef IFCOPSTRATEGY_HPP
#define IFCOPSTRATEGY_HPP

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <stack>

namespace NES::Optimizer {

class IFCOPStrategy : public BasePlacementStrategy {

  public:
    ~IFCOPStrategy() override = default;

    bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan) override;

    static std::unique_ptr<IFCOPStrategy> create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                 TopologyPtr topology,
                                                 TypeInferencePhasePtr typeInferencePhase,
                                                 StreamCatalogPtr streamCatalog);

  private:
    IFCOPStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                  TopologyPtr topology,
                  TypeInferencePhasePtr typeInferencePhase,
                  StreamCatalogPtr streamCatalog);

    std::vector<std::vector<bool>> getPlacementCandidate(NES::QueryPlanPtr queryPlan);

    double getCost(std::vector<std::vector<bool>> placementCandidate, NES::QueryPlanPtr queryPlan);

    double getLocalCost(std::vector<bool> nodePlacement, NES::QueryPlanPtr queryPlan);

    double getNetworkCost(const TopologyNodePtr& currentNode, std::vector<std::vector<bool>> placementCandidate, NES::QueryPlanPtr queryPlan);

    std::map<uint64_t, uint64_t> topologyNodeIdToIndexMap;
};
}// namespace NES::Optimizer
#endif//IFCOPSTRATEGY_HPP
