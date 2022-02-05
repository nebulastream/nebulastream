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

#ifndef NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_PLACEMENTSTRATEGYFACTORY_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_PLACEMENTSTRATEGYFACTORY_HPP_

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Util/PlacementStrategy.hpp>
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

class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;
}// namespace NES

namespace NES::Optimizer {

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class PlacementStrategyFactory {

  public:
    /**
     * @brief Factory method returning different kind of optimizer.
     * @param strategyName : name of the strategy
     * @param topology : topology information
     * @param globalExecutionPlan : global execution plan to be updated
     * @param typeInferencePhase : type inference phase instance
     * @param z3Context : context from the z3 library used for optimization
     * @return instance of type BaseOptimizer
     */
    static BasePlacementStrategyPtr getStrategy(PlacementStrategy::Value placementStrategy,
                                                const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                const TopologyPtr& topology,
                                                const TypeInferencePhasePtr& typeInferencePhase,
                                                const z3::ContextPtr& z3Context);
};
}// namespace NES::Optimizer
#endif// NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_PLACEMENTSTRATEGYFACTORY_HPP_
