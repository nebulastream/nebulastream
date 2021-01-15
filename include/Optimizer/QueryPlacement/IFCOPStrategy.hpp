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

#ifndef NES_IFCOPSTRATEGY_HPP
#define NES_IFCOPSTRATEGY_HPP

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/ExecutionPath.hpp>
#include <iostream>

namespace NES {

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

/**\brief:
 *          This class implements IFCOP placement strategy.
 */
class IFCOPStrategy : public BasePlacementStrategy {
  public:
    ~IFCOPStrategy(){};

    bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan);

    static std::unique_ptr<IFCOPStrategy> create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                                    TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog);

    // TODO: These should be private
    TopologyNodePtr generateRandomExecutionPath(TopologyPtr topology, QueryPlanPtr queryPlan);
    TopologyNodePtr getOptimizedExecutionPath(TopologyPtr topology, int maxIter, QueryPlanPtr queryPlan);

    std::map<TopologyNodePtr,std::vector<LogicalOperatorNodePtr>> getRandomAssignment(TopologyNodePtr executionPath,
                                                                                       std::vector<SourceLogicalOperatorNodePtr> sourceOperators);
  private:
    explicit IFCOPStrategy(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
        TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog);

    TopologyNodePtr runGlobalOptimization(QueryPlanPtr queryPlan, TopologyPtr topology, StreamCatalogPtr streamCatalog, int maxIter);
    float getTotalCost(TopologyNodePtr executionPath);
    float getExecutionPathCost(TopologyNodePtr executionPath);
    void placeNextOperator(TopologyNodePtr nextTopologyNodePtr, LogicalOperatorNodePtr nextOperatorPtr,
                           std::map<TopologyNodePtr , std::vector<LogicalOperatorNodePtr>>& nodeToOperatorsMap);
};
}// namespace NES

#endif//NES_IFCOPSTRATEGY_HPP
