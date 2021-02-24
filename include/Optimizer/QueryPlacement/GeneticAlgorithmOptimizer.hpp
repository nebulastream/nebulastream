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

#ifndef NES_GENETICALGORITHMOPTIMIZER_HPP
#define NES_GENETICALGORITHMOPTIMIZER_HPP

#include "BasePlacementStrategy.hpp"
namespace NES {

/**
 * brief: This class optimize the placement using a genetic algorithm method
 */
class GeneticAlgorithmOptimizer {

  public:
    typedef struct  {
        TopologyNodePtr topologyNode;
        LogicalOperatorNodePtr operatorNode;
        bool isPlaced;
    } PlacementItem;

    typedef std::vector<PlacementItem> Placement;

    GeneticAlgorithmOptimizer(QueryPlanPtr queryPlan, TopologyPtr topology, uint32_t populationSize,
                              float crossOverPosition, float mutationProbability, uint32_t numberOfMutatedGen,
                              std::map<LogicalOperatorNodePtr, float> ingestionRateModifiers,
                              std::map<LogicalOperatorNodePtr, float>  tupleSizeModifiers);

    ~GeneticAlgorithmOptimizer();

    Placement crossOver(Placement placement, Placement other);

    Placement mutate(Placement placement);

    float getCost(Placement placement);

    std::vector<Placement> population;

    Placement getOptimizedPlacement(uint32_t numberOfGeneration);

    static std::map<TopologyNodePtr, std::vector<LogicalOperatorNodePtr>> getOperatorAssignmentFromPlacement(Placement placement);

    static std::string getOperatorAssignmentAsString(Placement placement);

  private:

    uint32_t populationSize;
    float crossOverPosition;
    float mutationProbability;
    uint32_t numberOfMutatedGen;

    QueryPlanPtr queryPlan;
    TopologyPtr topology;

    std::map<LogicalOperatorNodePtr, float> ingestionRateModifiers;
    std::map<LogicalOperatorNodePtr, float>  tupleSizeModifiers;
};
}// namespace NES

#endif//NES_GENETICALGORITHMOPTIMIZER_HPP
