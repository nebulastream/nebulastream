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

#ifndef NES_INCLUDE_OPTIMIZER_QUERY_PLACEMENT_PLACEMENT_STRATEGY_FACTORY_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERY_PLACEMENT_PLACEMENT_STRATEGY_FACTORY_HPP_

#include <map>
#include <memory>

namespace z3 {
class expr;

class model;

class context;
using ContextPtr = std::shared_ptr<context>;

}// namespace z3

namespace NES {

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class StreamCatalog;
using StreamCatalogPtr = std::shared_ptr<StreamCatalog>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;
}// namespace NES

namespace NES::Optimizer {

enum NESPlacementStrategyType {
    TopDown,
    BottomUp,
    IFCOP,
    ILP,
    // FIXME: enable them with issue #755
    LowLatency,
    HighThroughput,
    MinimumResourceConsumption,
    MinimumEnergyConsumption,
    HighAvailability
};

static std::map<std::string, NESPlacementStrategyType> stringToPlacementStrategyType{
    {"BottomUp", BottomUp},
    {"TopDown", TopDown},
    {"IFCOP", IFCOP},
    {"ILP", ILP},
    // FIXME: enable them with issue #755
    //    {"Latency", LowLatency},
    //    {"HighThroughput", HighThroughput},
    //    {"MinimumResourceConsumption", MinimumResourceConsumption},
    //    {"MinimumEnergyConsumption", MinimumEnergyConsumption},
    //    {"HighAvailability", HighAvailability},
};

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class BasePlacementStrategy;
using BasePlacementStrategyPtr = std::shared_ptr<BasePlacementStrategy>;

class PlacementStrategyFactory {

  public:
    /**
     * @brief Factory method returning different kind of optimizer.
     * @param strategyName : name of the strategy
     * @param topology : topology information
     * @param globalExecutionPlan : global execution plan to be updated
     * @param typeInferencePhase : type inference phase instance
     * @param streamCatalog : stream catalog
     * @param z3Context : context from the z3 library used for optimization
     * @return instance of type BaseOptimizer
     */
    static std::unique_ptr<BasePlacementStrategy> getStrategy(const std::string& strategyName,
                                                              const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                              const TopologyPtr& topology,
                                                              const TypeInferencePhasePtr& typeInferencePhase,
                                                              const StreamCatalogPtr& streamCatalog,
                                                              const z3::ContextPtr& z3Context);

    /**
     * @brief Factory method returning different kind of optimizer (without the z3 optimizer).
     * @param strategyName : name of the strategy
     * @param topology : topology information
     * @param globalExecutionPlan : global execution plan to be updated
     * @param typeInferencePhase : type inference phase instance
     * @param streamCatalog : stream catalog
     * @return instance of type BaseOptimizer
     */
    static std::unique_ptr<BasePlacementStrategy> getStrategy(const std::string& strategyName,
                                                              const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                              const TopologyPtr& topology,
                                                              const TypeInferencePhasePtr& typeInferencePhase,
                                                              const StreamCatalogPtr& streamCatalog);
};
}// namespace NES::Optimizer
#endif  // NES_INCLUDE_OPTIMIZER_QUERY_PLACEMENT_PLACEMENT_STRATEGY_FACTORY_HPP_
