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

#include <Optimizer/QueryPlacement/GeneticAlgorithmOptimizer.hpp>
#include <Optimizer/QueryPlacement/IFCOPStrategy.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <math.h>

namespace NES {

GeneticAlgorithmOptimizer::GeneticAlgorithmOptimizer(QueryPlanPtr queryPlan, TopologyPtr topology, uint32_t populationSize,
                                                     float crossOverPosition, float mutationProbability, uint32_t numberOfMutatedGen,
                                                     std::map<LogicalOperatorNodePtr, float> ingestionRateModifiers,
                                                     std::map<LogicalOperatorNodePtr, float>  tupleSizeModifiers)
: queryPlan(queryPlan), topology(topology), populationSize(populationSize), crossOverPosition(crossOverPosition),
mutationProbability(mutationProbability), numberOfMutatedGen(numberOfMutatedGen), ingestionRateModifiers(ingestionRateModifiers),
tupleSizeModifiers(tupleSizeModifiers) {
    NES_DEBUG("GeneticAlgorithmOptimizer" << topology->toString());

    // Generate initial population
    std::vector<TopologyNodePtr> allTopologyNodes;
    std::vector<LogicalOperatorNodePtr> allOperatorNodes;

    // Identifying all topologyNodes
    std::vector<TopologyNodePtr> topologyNodesToVisit = {topology->getRoot()};
    while (topologyNodesToVisit.size() != 0) {
        TopologyNodePtr currentTopologyNode = topologyNodesToVisit.back();
        topologyNodesToVisit.pop_back();

        allTopologyNodes.push_back(currentTopologyNode);

        if (!currentTopologyNode->getChildren().empty()) {
            for (const auto child : currentTopologyNode->getChildren()) {
                topologyNodesToVisit.push_back(child->as<TopologyNode>());
            }
        }
    }

    // Identifying all LogicalOperatorNodePtr
    std::vector<LogicalOperatorNodePtr> operatorNodesToVisit = {queryPlan->getRootOperators()[0]->as<LogicalOperatorNode>()};
    while (operatorNodesToVisit.size() != 0) {
        LogicalOperatorNodePtr currentOperatorNode = operatorNodesToVisit.back();
        operatorNodesToVisit.pop_back();

        allOperatorNodes.push_back(currentOperatorNode);

        if (!currentOperatorNode->getChildren().empty()) {
            for (const auto child : currentOperatorNode->getChildren()) {
                operatorNodesToVisit.push_back(child->as<LogicalOperatorNode>());
            }
        }
    }

    NES_DEBUG("GeneticAlgorithmOptimizer: size of allTopologyNodes=" << allTopologyNodes.size());
    NES_DEBUG("GeneticAlgorithmOptimizer: size of allOperatorNodes=" << allOperatorNodes.size());

    // Generating random initial population
    srand(time(0));
    for (int i = 0; i < populationSize; i++) {
        Placement currentPlacement;
        for (TopologyNodePtr topologyNode : allTopologyNodes) {
            for (LogicalOperatorNodePtr operatorNode : allOperatorNodes) {
                currentPlacement.push_back({topologyNode, operatorNode, rand() % 2 == 0});
            }
        }
        float currentCost = getCost(currentPlacement);
        auto pos = std::find_if(population.begin(), population.end(), [currentCost, this](auto s) {
          return getCost(s) < currentCost;
        });
        population.insert(pos, currentPlacement);
    }

    NES_DEBUG("GeneticAlgorithmOptimizer: generated population size=" << population.size());
}

GeneticAlgorithmOptimizer::~GeneticAlgorithmOptimizer() {
    NES_DEBUG("~GeneticAlgorithmOptimizer");
}
GeneticAlgorithmOptimizer::Placement GeneticAlgorithmOptimizer::mutate(GeneticAlgorithmOptimizer::Placement placement) {
    Placement mutatedPlacement = placement;

    // select random index of gen to mutate
    std::vector<uint32_t> randomMutationIndex;
    srand(time(0));
    for (int i = 0; i < numberOfMutatedGen; i++) {
        float r = static_cast <float> (rand()) / static_cast <float> (RAND_MAX);
        if (r < mutationProbability) {
            uint32_t mutationIdx = rand() % placement.size();
            NES_DEBUG("GeneticAlgorithmOptimizer: mutationIdx=" << mutationIdx);
            bool originalValue = placement[mutationIdx].isPlaced;
            mutatedPlacement[mutationIdx].isPlaced = !originalValue;
        }
    }

    return mutatedPlacement;
}
GeneticAlgorithmOptimizer::Placement GeneticAlgorithmOptimizer::crossOver(GeneticAlgorithmOptimizer::Placement placement,
                                                                          GeneticAlgorithmOptimizer::Placement other) {
    Placement offspring;
    int crossOverIndex = std::ceil(crossOverPosition*placement.size());
    NES_DEBUG("GeneticAlgorithmOptimizer: crossOverIndex=" << crossOverIndex);
    for (int i=0; i < placement.size(); i++){
        if (i < crossOverIndex) {
            offspring.push_back(placement[i]);
        } else {
            offspring.push_back(other[i]);
        }
    }

    return offspring;
}
float GeneticAlgorithmOptimizer::getCost(GeneticAlgorithmOptimizer::Placement placement) {
    std::map<TopologyNodePtr, std::vector<LogicalOperatorNodePtr>> operatorAssignment = getOperatorAssignmentFromPlacement(placement);

    NES_DEBUG("GeneticAlgorithmOptimizer: operatorAssignment size=" << operatorAssignment.size());

    float cost = IFCOPStrategy::getTotalCost(queryPlan,
                                             topology,
                                             topology->getRoot(),
                                             operatorAssignment,
                                             ingestionRateModifiers,
                                             tupleSizeModifiers);
    NES_DEBUG("GeneticAlgorithmOptimizer: network cost=" << cost);

    return cost;
}

std::map<TopologyNodePtr, std::vector<LogicalOperatorNodePtr>>
GeneticAlgorithmOptimizer::getOperatorAssignmentFromPlacement(Placement placement) {
    // Build operator assignment from the placement
    std::map<TopologyNodePtr, std::vector<LogicalOperatorNodePtr>> operatorAssignment;

    for (PlacementItem placementItem: placement) {
        // add only if the current operator is placed in the current topology node
        if (placementItem.isPlaced) {
            // check if we have the topology node of the current placement item in the operatorAssignment map
            if (operatorAssignment.count(placementItem.topologyNode)==0) {

                std::vector<LogicalOperatorNodePtr> assignedOperators = {placementItem.operatorNode};
                // add the new key
                auto entry = std::make_pair(placementItem.topologyNode, assignedOperators);

                operatorAssignment.insert(entry);

            } else {
                // append the vector of this key
                std::vector<LogicalOperatorNodePtr> assignedOperators = operatorAssignment[placementItem.topologyNode];
                assignedOperators.push_back(placementItem.operatorNode);
            }
        }
    };

    return operatorAssignment;
}



GeneticAlgorithmOptimizer::Placement GeneticAlgorithmOptimizer::getOptimizedPlacement(uint32_t numberOfGeneration) {
    // cross over and mutate offspring
    for (int i = 0; i < numberOfGeneration; i++) {
        // cross over placements from the current generation
        for (int j = 0; j < population.size(); j++) {
            for (int k=0; k < population.size(); k++) {
                Placement currentOffspring = mutate(crossOver(population[i], population[j]));
                float currentCost = getCost(currentOffspring);
                auto pos = std::find_if(population.begin(), population.end(), [currentCost, this](auto s) {
                  return getCost(s) < currentCost;
                });
                // insert the current offspring to the population
                population.insert(pos, currentOffspring);

                population.pop_back();
            }
        }
    }
    NES_DEBUG("GeneticAlgorithmOptimizer best cost=" << getCost(population[0]));
    std::vector<float> costs;
    for (Placement placement : population){
        costs.push_back(getCost(placement));
    }
    return population[0];
}
std::string
GeneticAlgorithmOptimizer::getOperatorAssignmentAsString(Placement placement) {
    std::map<TopologyNodePtr, std::vector<LogicalOperatorNodePtr>> operatorAssignment = getOperatorAssignmentFromPlacement(placement);
    std::stringstream ss;
    for (std::map<TopologyNodePtr, std::vector<LogicalOperatorNodePtr>>::iterator iter = operatorAssignment.begin(); iter != operatorAssignment.end(); ++iter){
        ss << "TopologyNode id=" << iter->first->getId() << " OperatorIds:";
        for (LogicalOperatorNodePtr logicalOperatorNode: iter->second){
            ss << logicalOperatorNode->getId() << " " ;
        }
        ss << "\n";
    }
    return ss.str();
}

}// namespace NES
