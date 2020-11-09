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

#ifndef NES_QUERYPLACEMENTPHASE_HPP
#define NES_QUERYPLACEMENTPHASE_HPP

#include <memory>

namespace NES {

class QueryPlacementPhase;
typedef std::shared_ptr<QueryPlacementPhase> QueryPlacementPhasePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

class TypeInferencePhase;
typedef std::shared_ptr<TypeInferencePhase> TypeInferencePhasePtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

/**
 * @brief This class is responsible for placing operators of an input query plan on a global execution plan.
 */
class QueryPlacementPhase {
  public:
    static QueryPlacementPhasePtr create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                         TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog);

    /**
     * @brief Method takes input as a placement strategy name and input query plan and performs query operator placement based on the
     * selected query placement strategy
     * @param placementStrategy : name of the placement strategy
     * @param queryPlan : the query plan
     * @return true is placement successful.
     * @throws QueryPlacementException
     */
    bool execute(std::string placementStrategy, QueryPlanPtr queryPlan);
    ~QueryPlacementPhase();

  private:
    explicit QueryPlacementPhase(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                 TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog);
    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    StreamCatalogPtr streamCatalog;
};
}// namespace NES
#endif//NES_QUERYPLACEMENTPHASE_HPP
