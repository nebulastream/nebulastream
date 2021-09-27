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

#ifndef GENETICALGORITHM_HPP
#define GENETICALGORITHM_HPP

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <iostream>
#include <list>
#include <set>

namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;
}// namespace NES

namespace NES::Optimizer {

class GeneticAlgorithmStrategy : public BasePlacementStrategy {
  public:
    ~GeneticAlgorithmStrategy() override = default;

    bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan) override;

    static std::unique_ptr<GeneticAlgorithmStrategy> create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                            TopologyPtr topology,
                                                            TypeInferencePhasePtr typeInferencePhase,
                                                            StreamCatalogPtr streamCatalog);

  private:
    std::set<int> sourceNodesIndices;
    std::map<int, std::vector<int>> mapOfOperatorToSourceNodes;
    std::vector<TopologyNodePtr> topologyNodes;
    std::vector<NodePtr> queryOperators;
    std::vector<bool> transitiveClosureOfTopology;//transitive closure of the topology (reachability matrix)
    std::vector<double> operatorDMF;
    std::vector<int> operatorLoad;
    int numOfOperators;
    struct Placement {
        std::vector<bool> chromosome;
        double cost;
    };
    std::vector<Placement> population;

    explicit GeneticAlgorithmStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                      TopologyPtr topology,
                                      TypeInferencePhasePtr typeInferencePhase,
                                      StreamCatalogPtr streamCatalog);

    void setMapOfOperatorToSourceNodes(int queryId, std::vector<SourceLogicalOperatorNodePtr> sourceOperators);


    void placeQueryPlan(QueryPlanPtr queryPlan);

    std::vector<bool> getTransitiveClosure();

    void initializePopulation(QueryPlanPtr queryPlan);

    Placement generatePlacement(QueryPlanPtr queryPlan);

    double getCost(Placement placement);

    Placement getOptimizedPlacement(std::vector<Placement> population,uint32_t time, QueryPlanPtr queryPlan);

    Placement mutate(Placement placement, QueryPlanPtr queryPlan);

    void DFS(int s, int v, std::list<int>* adj, std::vector<bool>& tc, int numOfTopologyNodes);

    bool checkPlacementValidation(Placement placement, QueryPlanPtr queryPlan);
    std::vector<TopologyNodePtr> topologySnapshot(TopologyPtr topology);
    //void eliminateReachableNodes(std::vector<int>* topologyIndices);
    Placement crossOver(GeneticAlgorithmStrategy::Placement placement, GeneticAlgorithmStrategy::Placement other,uint32_t crossOverIndex);
    bool placementAlreadyExists(std::vector<GeneticAlgorithmStrategy::Placement> population, GeneticAlgorithmStrategy::Placement offpring);
};
}// namespace NES::Optimizer

#endif//GENETICALGORITHM_HPP