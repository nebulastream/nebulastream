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

#ifndef NES_TOPOLOGYRECONFIGURATIONPHASE_HPP
#define NES_TOPOLOGYRECONFIGURATIONPHASE_HPP

#include <memory>
#include <Util/PlacementStrategy.hpp>
namespace NES {

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class SharedQueryPlan;
using SharedQueryPlanPtr = std::shared_ptr<SharedQueryPlan>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

namespace Optimizer {
class QueryPlacementPhase;
using QueryPlacementPhasePtr = std::shared_ptr<QueryPlacementPhase>;
}

namespace Optimizer::Experimental {

class TopologyReconfigurationPhase;
using TopologyReconfigurationPhasePtr = std::shared_ptr<TopologyReconfigurationPhase>;

class TopologyReconfigurationPhase {
  public:
    TopologyReconfigurationPhasePtr create();

    bool execute(PlacementStrategy::Value placementStrategy, const SharedQueryPlanPtr& sharedQueryPlan, uint64_t movingNode, uint64_t oldParent, uint64_t newParent);

    explicit TopologyReconfigurationPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                          TopologyPtr topology);
  private:

    TopologyNodePtr getCommonAncestor(uint64_t oldParent, uint64_t newParent);
    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    QueryPlacementPhasePtr queryPlacementPhase;
};
}
}

#endif//NES_TOPOLOGYRECONFIGURATIONPHASE_HPP
