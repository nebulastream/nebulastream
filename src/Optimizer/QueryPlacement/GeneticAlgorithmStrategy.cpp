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
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/GeneticAlgorithmStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>

namespace NES::Optimizer {

std::unique_ptr<GeneticAlgorithmStrategy> GeneticAlgorithmStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                                           TopologyPtr topology,
                                                                           TypeInferencePhasePtr typeInferencePhase,
                                                                           StreamCatalogPtr streamCatalog) {
    return std::make_unique<GeneticAlgorithmStrategy>(GeneticAlgorithmStrategy(std::move(globalExecutionPlan),
                                                                               std::move(topology),
                                                                               std::move(typeInferencePhase),
                                                                               std::move(streamCatalog)));
}

GeneticAlgorithmStrategy::GeneticAlgorithmStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                                   TopologyPtr topology,
                                                   TypeInferencePhasePtr typeInferencePhase,
                                                   StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(std::move(globalExecutionPlan),
                            std::move(topology),
                            std::move(typeInferencePhase),
                            std::move(streamCatalog)) {}

bool GeneticAlgorithmStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan) {
    const QueryId queryId = queryPlan->getQueryId();
    topologyNodes = topologySnapshot(topology);
    queryOperators = QueryPlanIterator(queryPlan).snapshot();
    try {
        NES_INFO("GeneticAlgorithmStrategy: Performing placement of the input query plan with id " << queryId);
        NES_INFO("GeneticAlgorithmStrategy: And query plan \n" << queryPlan->toString());

        NES_DEBUG("GeneticAlgorithmStrategy: Get all source operators");
        const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
        if (sourceOperators.empty()) {
            NES_ERROR("GeneticAlgorithmStrategy: No source operators found in the query plan wih id: " << queryId);
            throw Exception("GeneticAlgorithmStrategy: No source operators found in the query plan wih id: "
                            + std::to_string(queryId));
        }

        NES_DEBUG("GeneticAlgorithmStrategy: map operators to topology source nodes");
        setMapOfOperatorToSourceNodes(queryId, sourceOperators);

        NES_DEBUG("GeneticAlgorithmStrategy: Get all sink operators");
        const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = queryPlan->getSinkOperators();
        if (sinkOperators.empty()) {
            NES_ERROR("GeneticAlgorithmStrategy: No sink operators found in the query plan wih id: " << queryId);
            throw Exception("GeneticAlgorithmStrategy: No sink operators found in the query plan wih id: "
                            + std::to_string(queryId));
        }

        NES_DEBUG("GeneticAlgorithmStrategy: place query plan with id : " << queryId);
        placeQueryPlan(queryPlan);
        NES_DEBUG("GeneticAlgorithmStrategy: Add system generated operators for query with id : " << queryId);
        addNetworkSourceAndSinkOperators(queryPlan);
        NES_DEBUG("GeneticAlgorithmStrategy: clear the temporary map : " << queryId);
        operatorToExecutionNodeMap.clear();
        pinnedOperatorLocationMap.clear();
        NES_DEBUG(
            "GeneticAlgorithmStrategy: Run type inference phase for query plans in global execution plan for query with id : "
            << queryId);

        NES_DEBUG("GeneticAlgorithmStrategy: Update Global Execution Plan : \n" << globalExecutionPlan->getAsString());
        return runTypeInferencePhase(queryId);
    } catch (Exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
}

void GeneticAlgorithmStrategy::placeQueryPlan(QueryPlanPtr queryPlan) {

    transitiveClosureOfTopology = getTransitiveClosure();
    //Get the DMF and load of each query operator and save it in the corresponding vector
    for (auto currentOperator : queryOperators) {
        operatorDMF.push_back(std::any_cast<double>(currentOperator->as<LogicalOperatorNode>()->getProperty("dmf")));
        operatorLoad.push_back(std::any_cast<int>(currentOperator->as<LogicalOperatorNode>()->getProperty("load")));
    }
    population.clear();
    initializePopulation(queryPlan);
    Placement optimizedPlacement;
    //check if the query contains more than one operator other than sink and
    int numOfSourceOperators = queryPlan->getSourceOperators().size();
    if(numOfOperators > numOfSourceOperators+2){
        optimizedPlacement = getOptimizedPlacement(population, 10, queryPlan);
    }else{
        optimizedPlacement = population[0];
    }

    //Placement optimizedPlacement = population[0];
    PlacementMatrix mapping;
    for (uint32_t i = 0; i < topologyNodes.size(); i++) {
        std::vector<bool> genome;
        auto assignmentBegin = optimizedPlacement.chromosome.begin() + i * queryOperators.size();
        genome.assign(assignmentBegin, assignmentBegin + queryOperators.size());
        mapping.push_back(genome);
    }

    assignMappingToTopology(topology, queryPlan, mapping);
}

void GeneticAlgorithmStrategy::initializePopulation(QueryPlanPtr queryPlan) {

    for (int i = 0; i < 100; i++) {
        Placement placement = generatePlacement(queryPlan);
        if(placementAlreadyExists(population,placement))
            continue;
        placement.cost = getCost(placement);
        auto pos = std::find_if(population.begin(), population.end(), [placement](auto s) {
            return s.cost > placement.cost;
        });
        population.insert(pos, placement);
        bool valid = checkPlacementValidation(placement, queryPlan);
        if (!valid) {
            NES_DEBUG("Placement is invalid ");
            exit(1);
        }
    }
}

GeneticAlgorithmStrategy::Placement GeneticAlgorithmStrategy::generatePlacement(QueryPlanPtr queryPlan) {
    Placement placement;
    uint32_t numOfTopologyNodes = topologyNodes.size();
    numOfOperators = queryOperators.size();
    std::vector<bool> chromosome(numOfOperators * numOfTopologyNodes);
    int candidateTopologyNodeIndex = 0;//the position of candidate nodes in "topologyNodes" vector
    uint32_t operatorIndex = 1;        // start from the second operator because the first operator is sink
    chromosome[0] = true;              //place the sink operator in the root node
    uint32_t numOfOperatorsToPlace = queryOperators.size() - queryPlan->getSinkOperators().size();
    std::vector<bool> visitedQueryOperators(queryOperators.size());
    visitedQueryOperators.at(0) = true;//sink operator is already placed
    srand(time(0));
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, 1);
    while (numOfOperatorsToPlace) {
        std::vector<int> operatorSourceNodeIndices = mapOfOperatorToSourceNodes.find(operatorIndex)->second;
        // Check if the current operator to place is a source operator
        if ((queryOperators[operatorIndex]->as<OperatorNode>()->instanceOf<SourceLogicalOperatorNode>())) {
            candidateTopologyNodeIndex = operatorSourceNodeIndices[0];
            chromosome[candidateTopologyNodeIndex * numOfOperators + operatorIndex] = true;
            operatorIndex += 1;
            numOfOperatorsToPlace -= 1;
            continue;
        }
        // Check if the current topology node is a source node
        if (std::find(operatorSourceNodeIndices.begin(), operatorSourceNodeIndices.end(), candidateTopologyNodeIndex)
            != operatorSourceNodeIndices.end()) {
            //assign all operators between the current operator and the source operator
            while (!queryOperators[operatorIndex]->as<OperatorNode>()->instanceOf<SourceLogicalOperatorNode>()) {
                chromosome[candidateTopologyNodeIndex * numOfOperators + operatorIndex] = true;
                visitedQueryOperators.at(operatorIndex) = true;
                operatorIndex += 1;
                numOfOperatorsToPlace -= 1;
            }
                //assign the source operator
                chromosome[candidateTopologyNodeIndex * numOfOperators + operatorIndex] = true;
                visitedQueryOperators.at(operatorIndex) = true;
                operatorIndex += 1;
                numOfOperatorsToPlace -= 1;
                continue;
        }
        if (!visitedQueryOperators.at(operatorIndex)) {
            OperatorNodePtr parentOperator = queryOperators.at(operatorIndex)
                                                 ->getParents()[0]
                                                 ->as<OperatorNode>();//We assume each operator to have only one parent
            auto it = std::find(queryOperators.begin(), queryOperators.end(), parentOperator);
            int parentOperatorIndex = std::distance(queryOperators.begin(), it);
            int parentOperatorTopologyIndex = -1;//topology nodes where the parent operator is assigned
            for (uint32_t i = 0; i < topologyNodes.size(); i++) {
                if (chromosome[i * numOfOperators + parentOperatorIndex]) {
                    parentOperatorTopologyIndex = i;
                    break;
                }
            }
            visitedQueryOperators.at(operatorIndex) = true;
            candidateTopologyNodeIndex = parentOperatorTopologyIndex;
        }

        int placeTheOperator = distrib(gen);
        if (!placeTheOperator) {
            int newCandidateNodeIndex = -1;
            auto children = topologyNodes[candidateTopologyNodeIndex]->getChildren();
            for (auto child : children) {
                auto iterator = std::find(topologyNodes.begin() + candidateTopologyNodeIndex, topologyNodes.end(), child);
                int childNodeIndex = std::distance(topologyNodes.begin(), iterator);
                bool validNode = true;
                for (auto operatorSourceNodeIndex : operatorSourceNodeIndices) {
                    if (!transitiveClosureOfTopology[childNodeIndex * numOfTopologyNodes + operatorSourceNodeIndex]) {
                        validNode = false;
                        break;
                    }
                }
                if (validNode) {
                    newCandidateNodeIndex = childNodeIndex;
                    break;
                }
            }
            if (newCandidateNodeIndex < 0) {
                chromosome[candidateTopologyNodeIndex * numOfOperators + operatorIndex] = true;
                operatorIndex += 1;
                numOfOperatorsToPlace -= 1;
                continue;
            }
            candidateTopologyNodeIndex = newCandidateNodeIndex;
        }// If placeTheOperator is false
        else {
            chromosome[candidateTopologyNodeIndex * numOfOperators + operatorIndex] = true;
            operatorIndex += 1;
            numOfOperatorsToPlace -= 1;
        }
    }
    placement.chromosome = chromosome;
    return placement;
}

GeneticAlgorithmStrategy::Placement
GeneticAlgorithmStrategy::getOptimizedPlacement(std::vector<Placement> population, uint32_t time, QueryPlanPtr queryPlan) {

    Placement optimizedPlacement = population[0];
    std::vector<GeneticAlgorithmStrategy::Placement> offspringPopulation;
    std::map<uint32_t, int> counts;
    std::map<uint32_t, uint32_t> populationSize;
    std::map<uint32_t, double> costsAfterGA;
     NES_DEBUG("Cost before Optimization:  " << population[0].cost);
    costsAfterGA.insert(std::make_pair(0, optimizedPlacement.cost));
    for (uint32_t i = 0; i < time; i++) {
        /* int i = 0;
    for (auto start = std::chrono::steady_clock::now(), now = start; now < start + std::chrono::seconds{time};
         now = std::chrono::steady_clock::now()) {
        int numOfInvalidOffsprings = 0; */
        // cross over placements from the current generation
        int numOfInvalidOffsprings = 0;
        uint32_t selectedPortion = 10;
        if (population.size() < selectedPortion)
            selectedPortion = population.size();

        for (uint32_t j = 0; j < selectedPortion; j++) {
            Placement parent1 = population[j];
            int beginPlacementParent1 = -1;
            int endPlacementParent1 = -1;
            for (uint32_t m = 1; m < parent1.chromosome.size(); m++) {
                if ( parent1.chromosome.at(m)) {
                    beginPlacementParent1 = m;
                    break;
                }
            }
            for (uint32_t m = parent1.chromosome.size()-2; m > 0 ; m--) {
                if ( parent1.chromosome.at(m)) {
                    endPlacementParent1 = m;
                    break;
                }
            }
            for (uint32_t k = j+1; k < selectedPortion; k++) {

                Placement parent2 = population[k];
                int beginPlacementParent2 = -1;
                int endPlacementParent2 = -1;
                for (uint32_t m = 1; m < parent2.chromosome.size(); m++) {
                    if ( parent2.chromosome.at(m)) {
                        beginPlacementParent2 = m;
                        break;
                    }
                }
                for (uint32_t m = parent2.chromosome.size()-2; m > 0 ; m--) {
                    if ( parent2.chromosome.at(m)) {
                        endPlacementParent2 = m;
                        break;
                    }
                }
                uint32_t begin = beginPlacementParent2;
                if(beginPlacementParent1 > beginPlacementParent2)
                    begin = beginPlacementParent1;
                uint32_t end = endPlacementParent2;
                if(endPlacementParent1 < endPlacementParent2)
                    end = endPlacementParent1;
                if(begin >= end)
                    continue;
                //srand(time(NULL));
                uint32_t crossOverIndex = (uint32_t) (begin + rand() % (end-begin));

                Placement offspring1 = crossOver(parent1, parent2,crossOverIndex);
                Placement offspring2 = crossOver(parent2, parent1,crossOverIndex);

                Placement tempOffspring1 = offspring1;
                Placement tempOffspring2 = offspring2;
                tempOffspring1.cost = (double) RAND_MAX;
                tempOffspring2.cost = (double) RAND_MAX;
                if (checkPlacementValidation(tempOffspring1, queryPlan)) {
                    tempOffspring1.cost = getCost(tempOffspring1);
                }
                else{
                    numOfInvalidOffsprings += 1;
                }
                if (checkPlacementValidation(tempOffspring2, queryPlan)) {
                    tempOffspring2.cost = getCost(tempOffspring2);
                }
                else{
                    numOfInvalidOffsprings += 1;
                }
                offspring1 = mutate(offspring1, queryPlan);
                offspring2 = mutate(offspring2, queryPlan);
                offspring1.cost = (double) RAND_MAX;
                offspring2.cost = (double) RAND_MAX;
                if (checkPlacementValidation(offspring1, queryPlan)) {
                    offspring1.cost = getCost(offspring1);
                }
                if (checkPlacementValidation(offspring2, queryPlan)) {
                    offspring2.cost = getCost(offspring2);
                }

                if(offspring1.cost > tempOffspring1.cost)
                    offspring1 = tempOffspring1;
                if(offspring2.cost > tempOffspring2.cost)
                    offspring2 = tempOffspring2;

                if (!placementAlreadyExists(offspringPopulation, offspring1)) {
                    auto pos = std::find_if(offspringPopulation.begin(), offspringPopulation.end(), [offspring1](auto s) {
                        return s.cost > offspring1.cost;
                    });
                    offspringPopulation.insert(pos, offspring1);
                }
                if (!placementAlreadyExists(offspringPopulation, offspring2)) {
                    auto pos = std::find_if(offspringPopulation.begin(), offspringPopulation.end(), [offspring2](auto s) {
                        return s.cost > offspring2.cost;
                    });
                    offspringPopulation.insert(pos, offspring2);
                }
            }
        }

        populationSize.insert(std::make_pair(i, selectedPortion));
        counts.insert(std::make_pair(i, numOfInvalidOffsprings));
        costsAfterGA.insert(std::make_pair(i + 1, offspringPopulation[0].cost));

        if (offspringPopulation.size() && optimizedPlacement.cost > offspringPopulation[0].cost) {
            optimizedPlacement = offspringPopulation[0];
        }
        //i += 1;
        population = offspringPopulation;
        offspringPopulation.clear();
    }
    /*
    NES_DEBUG("Cost Before Optimization:  " << costsAfterGA[0]);
    for (auto& [i, numOfInvalidOffsprings] : counts) {
        NES_DEBUG("Current Generation:  " << i+1);
        NES_DEBUG("Optimized Cost of this Generation:  " << costsAfterGA[i+1]);
        NES_DEBUG("Current Population Size:  " << populationSize.at(i));
        NES_DEBUG("Number of invalid offsprings:  " << numOfInvalidOffsprings);
        //NES_DEBUG("Best cost of this generation:  " << costs[i]);

    }
    */
    return optimizedPlacement;
}

GeneticAlgorithmStrategy::Placement GeneticAlgorithmStrategy::mutate(GeneticAlgorithmStrategy::Placement placement,
                                                                     QueryPlanPtr queryPlan) {
    Placement mutatedPlacement = placement;
    double mutationProbability = 1;
    auto sourceOperators = queryPlan->getSourceOperators();
    // select random index of gen to mutate
    std::vector<uint32_t> randomMutationIndex;
    srand(time(0));
    double r = static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
    uint32_t mutationOperatorIdx ;
    if (r < mutationProbability) {
        mutationOperatorIdx = rand() % queryOperators.size();
        while (!mutationOperatorIdx
               || (find(sourceOperators.begin(), sourceOperators.end(), queryOperators.at(mutationOperatorIdx))
                   != sourceOperators.end())) {
            mutationOperatorIdx = rand() % queryOperators.size();
        }
        uint32_t mutationNodeIdx = (uint32_t)RAND_MAX;
        for (uint32_t i = 0; i < topologyNodes.size(); i++) {
            if (placement.chromosome.at(i * queryOperators.size() + mutationOperatorIdx)){
                mutationNodeIdx = i;
                break;
            }
        }
        if(mutationNodeIdx == RAND_MAX)
            return placement;
        uint32_t newMutationNodeIdx = rand() % topologyNodes.size();
        uint32_t test = mutationNodeIdx*queryOperators.size() + mutationOperatorIdx;
        mutatedPlacement.chromosome.at(test) = false;
        mutatedPlacement.chromosome.at(newMutationNodeIdx*queryOperators.size() + mutationOperatorIdx) = true;
    }

    return mutatedPlacement;
}

std::vector<bool> GeneticAlgorithmStrategy ::getTransitiveClosure() {
    int numOfTopologyNodes = topologyNodes.size();
    std::vector<bool> transitiveClosureOfTopology(numOfTopologyNodes
                                                  * numOfTopologyNodes);// The transitive closure of the topology
    std::list<int>* adjList;                                            // array of adjacency lists
    adjList = new std::list<int>[numOfTopologyNodes];

    for (int i = 0; i < numOfTopologyNodes; i++) {
        auto current = topologyNodes[i];
        auto children = topologyNodes[i]->getChildren();
        for (uint32_t j = 0; j < children.size(); j++) {
            auto it = std::find(topologyNodes.begin(), topologyNodes.end(), children[j]->as<TopologyNode>());
            int pos = std::distance(topologyNodes.begin(), it);
            adjList[i].push_back(pos);
        }
    }
    for (int i = 0; i < numOfTopologyNodes; i++) {
        DFS(i, i, adjList, transitiveClosureOfTopology, numOfTopologyNodes);
    }
    return transitiveClosureOfTopology;
}// transitiveClosure

void GeneticAlgorithmStrategy ::DFS(int s,
                                    int v,
                                    std::list<int>* adjList,
                                    std::vector<bool>& transitiveClosureOfTopology,
                                    int numOfTopologyNodes) {
    transitiveClosureOfTopology.at(s * numOfTopologyNodes + v) = true;

    // Find all the nodes that can reach s through v
    std::list<int>::iterator itr;
    for (itr = adjList[v].begin(); itr != adjList[v].end(); ++itr) {
        int adjNode = *itr;
        if (!transitiveClosureOfTopology.at(s * numOfTopologyNodes + adjNode))
            DFS(s, adjNode, adjList, transitiveClosureOfTopology, numOfTopologyNodes);
    }
}//DFS

bool GeneticAlgorithmStrategy::checkPlacementValidation(GeneticAlgorithmStrategy::Placement placement, QueryPlanPtr queryPlan) {
    std::vector<bool> chromosome = placement.chromosome;
    int numOfTopologyNodes = topologyNodes.size();
    auto queryOperators = QueryPlanIterator(queryPlan).snapshot();
    int numOfOperators = queryOperators.size();

    //For each query operator check if the placement is valid
    for (int i = 0; i < numOfOperators; i++) {
        std::vector<int> childrenOperatorsNodesIndices;
        std::vector<int> currentOperatorsNodeIndices ;
        int currentOperatorsNodeIndex;
        std::vector<int> childrenOperatorsPositions;
        auto childrenOperators = queryOperators[i]->getChildren();

        //Get the topology node where the current operator is assigned to
        for (int j = 0; j < numOfTopologyNodes; j++) {
            if (chromosome[(j * queryOperators.size()) + i]){
                currentOperatorsNodeIndices.push_back(j);
            }
        }
        //Check if the current query operator is assigned to only one topology node
        if(currentOperatorsNodeIndices.size() == 1){
            currentOperatorsNodeIndex = currentOperatorsNodeIndices[0];
        }
        else{
            return false;
        }

        //Get the indices of the children operators
        for (auto child : childrenOperators) {
            auto it = std::find(queryOperators.begin(), queryOperators.end(), child);
            int pos = std::distance(queryOperators.begin(), it);
            childrenOperatorsPositions.push_back(pos);
        }

        //Get the topology nodes where the children query operators are assigned to
        for (auto childIndex : childrenOperatorsPositions) {
            int childOperatorNodeIndex = -1;
            for (int j = 0; j < numOfTopologyNodes; j++) {
                if (chromosome[(j * queryOperators.size()) + childIndex]) {
                    childOperatorNodeIndex = j;
                    break;
                }
            }
            if(childOperatorNodeIndex >= 0 && !transitiveClosureOfTopology.at(currentOperatorsNodeIndex * numOfTopologyNodes + childOperatorNodeIndex))
                return false;
        }

    }
    return true;
}

std::vector<TopologyNodePtr> GeneticAlgorithmStrategy::topologySnapshot(TopologyPtr topology) {
    std::vector<TopologyNodePtr> nodes;
    auto topologyIterator = NES::DepthFirstNodeIterator(topology->getRoot()).begin();

    while (topologyIterator != DepthFirstNodeIterator::end()) {
        // get the ExecutionNode for the current topology Node
        auto currentTopologyNode = (*topologyIterator)->as<TopologyNode>();

        nodes.push_back(currentTopologyNode);
        ++topologyIterator;
    }
    return nodes;
}
void GeneticAlgorithmStrategy ::setMapOfOperatorToSourceNodes(int queryId,
                                                              std::vector<SourceLogicalOperatorNodePtr> sourceOperators) {
    // map of logical stream names to source nodes
    std::map<std::string, std::vector<int>> mapOfStreamNameToSourceNodes;
    for (auto sourceOperator : sourceOperators) {
        std::vector<int> streamSourceNodesIndices;
        if (!sourceOperator->getSourceDescriptor()->hasStreamName()) {
            throw Exception("GeneticAlgorithmStrategy: Source Descriptor need stream name: " + std::to_string(queryId));
        }
        const std::string streamName = sourceOperator->getSourceDescriptor()->getStreamName();
        if (mapOfStreamNameToSourceNodes.find(streamName) != mapOfStreamNameToSourceNodes.end()) {
            NES_TRACE("GeneticAlgorithmStrategy: Entry for the logical stream " << streamName << " already present.");
            continue;
        }
        NES_TRACE("GeneticAlgorithmStrategy: Get all topology nodes for the logical source stream");
        const std::vector<TopologyNodePtr> sourceNodes = streamCatalog->getSourceNodesForLogicalStream(streamName);
        if (sourceNodes.empty()) {
            NES_ERROR("BasePlacementStrategy: No source found in the topology for stream " << streamName
                                                                                           << " for query with id : " << queryId);
            throw Exception("BasePlacementStrategy: No source found in the topology for stream " + streamName
                            + " for query with id : " + std::to_string(queryId));
        }

        for (auto sourceNode : sourceNodes) {

            auto it = std::find(topologyNodes.begin(), topologyNodes.end(), sourceNode->as<TopologyNode>());
            int index = std::distance(topologyNodes.begin(), it);
            streamSourceNodesIndices.push_back(index);
            sourceNodesIndices.insert(index);
        }
        mapOfStreamNameToSourceNodes[streamName] = streamSourceNodesIndices;
    }
    // map of query operator to logical stream names
    std::map<int, std::vector<int>> mapOfOperatorToSourceOperatorId;
    // map of source operator to topology node
    std::map<int, int> mapOfSrcOperatorToTopologyNodeIdx;

    for (auto sourceOperator : sourceOperators) {
        const std::string streamName = sourceOperator->getSourceDescriptor()->getStreamName();
        std::vector<int> streamSourceNodesIndices = mapOfStreamNameToSourceNodes[streamName];
        if (streamSourceNodesIndices.empty()) {
            NES_ERROR("BasePlacementStrategy: unable to find topology nodes for logical source " << streamName);
            throw Exception("BasePlacementStrategy: unable to find topology nodes for logical source " + streamName);
        }
        TopologyNodePtr candidateTopologyNode = topologyNodes[streamSourceNodesIndices[0]];
        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
            throw Exception(
                "BasePlacementStrategy: Unable to find resources on the physical node for placement of source operator");
        }
        mapOfSrcOperatorToTopologyNodeIdx[sourceOperator->getId()] = streamSourceNodesIndices[0];
        streamSourceNodesIndices.erase(streamSourceNodesIndices.begin());
        mapOfStreamNameToSourceNodes[streamName] = streamSourceNodesIndices;

        std::vector<NodePtr> streamOperators = {sourceOperator};//parentOperators;
        std::vector<NodePtr> workStack = {sourceOperator};
        while (!workStack.empty()) {
            auto parents = workStack.front()->getParents();
            workStack.insert(workStack.end(), parents.begin(), parents.end());
            streamOperators.insert(streamOperators.end(), parents.begin(), parents.end());
            workStack.erase(workStack.begin());
        }
        for (auto streamOperator : streamOperators) {
            auto iterator = std::find(queryOperators.begin(), queryOperators.end(), streamOperator);
            int index = std::distance(queryOperators.begin(), iterator);
            if (mapOfOperatorToSourceOperatorId.find(index) != mapOfOperatorToSourceOperatorId.end()) {
                if (std::find(mapOfOperatorToSourceOperatorId[index].begin(),
                              mapOfOperatorToSourceOperatorId[index].end(),
                              sourceOperator->getId())
                    != mapOfOperatorToSourceOperatorId[index].end()) {
                    continue;
                } else {
                    mapOfOperatorToSourceOperatorId[index].push_back(sourceOperator->getId());
                }
            } else {
                mapOfOperatorToSourceOperatorId[index].push_back(sourceOperator->getId());
            }
        }
    }
    // map of query operator to topology source nodes
    for (uint32_t i = 0; i < queryOperators.size(); i++) {
        auto sourceOperatorsIds = mapOfOperatorToSourceOperatorId.find(i)->second;
        for (auto sourceOperatorsId : sourceOperatorsIds) {
            auto operatorSourceNodesIndices = mapOfSrcOperatorToTopologyNodeIdx.find(sourceOperatorsId)->second;
            mapOfOperatorToSourceNodes[i].push_back(operatorSourceNodesIndices);
        }
    }
}
double GeneticAlgorithmStrategy::getCost(Placement placement) {
    int numberOfOperators = queryOperators.size();
    //global cost of nodes in the placement
    std::map<NodePtr, double> globalNodesCost;
    //std::vector<TopologyNodePtr> placementTopologyNodes = topologySnapshot(placement.topology);
    //Iterate over all nodes in the placement to calculate the global cost of each node
    for (int i = topologyNodes.size() - 1; i >= 0; i--) {
        int assignment_begin = i * numberOfOperators;
        double localCost = 1;    //if no operators are assigned to the  current node then the local cost is 1

        int nodeLoad = 0;

        //check which operators are assigned to the current node and add the cost of these operators to the local cost of the current node
        for (int j = 0; j < numberOfOperators; j++) {
            if (placement.chromosome[assignment_begin+j]) {
                localCost *= operatorDMF.at(j);
                nodeLoad += operatorLoad.at(j);
            }
        }

        //check wether the current node is overloaded if yes then add the overloading penalization  the local cost
        auto availableCapacity = topologyNodes.at(i)->getAvailableResources();
        if (nodeLoad > availableCapacity)
            localCost += (nodeLoad - availableCapacity);

        //Add the global cost of the children nodes to the global cost of the current node
        double childGlobalCost = 0;
        double globalCost = 0;
        auto children = topologyNodes.at(i)->getChildren();
        for (auto& child : children) {
            childGlobalCost = globalNodesCost.at(child);
            globalCost += localCost * childGlobalCost;
        }
        //In case the current node has no children then the global cost is the local cost
        if (globalCost == 0)
            globalCost = localCost;
        globalNodesCost.insert(std::make_pair(topologyNodes.at(i), globalCost));
    }
    double cost = 0;
    std::map<NodePtr, double>::iterator it;
    for (it = globalNodesCost.begin(); it != globalNodesCost.end(); it++) {
        cost += it->second;
    }
    // return globalNodesCost[placementTopologyNodes.at(0)];
    return cost;
}

GeneticAlgorithmStrategy::Placement GeneticAlgorithmStrategy::crossOver(GeneticAlgorithmStrategy::Placement placement,
                                                                        GeneticAlgorithmStrategy::Placement other, uint32_t crossOverIndex) {

    Placement offspring = placement;
    std::vector<bool> offspringChromosome;
    for (uint32_t i = 0; i < placement.chromosome.size(); i++) {
        if (i <= crossOverIndex) {
            offspringChromosome.push_back(placement.chromosome[i]);
        } else {
            offspringChromosome.push_back(other.chromosome[i]);
        }
    }
    offspring.chromosome = offspringChromosome;
    return offspring;
}
bool GeneticAlgorithmStrategy::placementAlreadyExists(std::vector<GeneticAlgorithmStrategy::Placement> population,
                                              GeneticAlgorithmStrategy::Placement offspring) {
    auto populationIterator = population.begin();
    while (populationIterator != population.end()) {
        if ( std::equal(offspring.chromosome.begin(), offspring.chromosome.end(), (*populationIterator).chromosome.begin())) {
            return true;
        }
        ++populationIterator;
    }
    return false;
}
}// namespace NES::Optimizer

