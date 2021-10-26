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
#include <Util/yaml/Yaml.hpp>
#include <filesystem>

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
    mapOfBreadthFirstToDepthFirst = breadthFirstNodeIterator(topology);
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
    //topology->print();
    //NES_DEBUG(queryPlan->toString());
    transitiveClosureOfTopology = getTransitiveClosure();
    //Get the DMF and load of each query operator and save it in the corresponding vector
    for (auto currentOperator : queryOperators) {
        operatorDMF.push_back(std::any_cast<double>(currentOperator->as<LogicalOperatorNode>()->getProperty("dmf")));
        operatorLoad.push_back(std::any_cast<int>(currentOperator->as<LogicalOperatorNode>()->getProperty("load")));
    }

    uint32_t populationSize = 1000;
    uint32_t numOfIterations = 10;
    uint32_t patience = 3;
    double threshold = 0.0001;
    uint32_t numOfGenesToMutate = 1;
    double mutationProbability = 0.5;
    const std::string filePath = "../tests/test_data/GeneticAlgorithmParameters.yaml";
    if (!filePath.empty() && std::filesystem::exists(filePath)) {

        NES_INFO("GeneticAlgorithmConfig: Using config file with path: " << filePath << " .");

        Yaml::Node config = *(new Yaml::Node());
        Yaml::Parse(config, filePath.c_str());
        try {
            if (!config["populationSize"].As<std::string>().empty() && config["populationSize"].As<std::string>() != "\n") {
                populationSize = config["populationSize"].As<uint32_t>();
            }
            if (!config["numOfIterations"].As<std::string>().empty() && config["numOfIterations"].As<std::string>() != "\n") {
                numOfIterations = config["numOfIterations"].As<uint32_t>();
            }
            if (!config["patience"].As<std::string>().empty() && config["patience"].As<std::string>() != "\n") {
                patience = config["patience"].As<uint32_t>();
            }
            if (!config["threshold"].As<std::string>().empty() && config["threshold"].As<std::string>() != "\n") {
                threshold = config["threshold"].As<double>();
            }
            if (!config["numOfGenesToMutate"].As<std::string>().empty() && config["numOfGenesToMutate"].As<std::string>() != "\n") {
                numOfGenesToMutate = config["numOfGenesToMutate"].As<uint32_t>();
            }
            if (!config["mutationProbability"].As<std::string>().empty() && config["mutationProbability"].As<std::string>() != "\n") {
                mutationProbability = config["mutationProbability"].As<double>();
            }
        } catch (std::exception& e) {
            NES_ERROR("Genetic Algorithm Configuration: Error while initializing configuration parameters from YAML file. Keeping default "
                      "values. "
                      << e.what());
        }
    }
    else{
        NES_ERROR("Genetic Algorithm Configuration: No file path was provided or file could not be found at " << filePath << ".");
        NES_WARNING("Keeping default values for Genetic Algorithm Configuration.");
    }
    population.clear();
    initializePopulation(queryPlan, populationSize);
    Placement initialPlacement = population[0];
    Placement optimizedPlacement  = initialPlacement;
    // check if the query has at least 2 other operators than source and sink operators
    if(queryOperators.size() > 2 + sourceNodesIndices.size()){
        optimizedPlacement  = getOptimizedPlacement(population, numOfIterations , patience, threshold, numOfGenesToMutate, mutationProbability, queryPlan);
    }
    NES_DEBUG("Best Initial Placement: ");
    // print the placement
    for(uint32_t f = 0 ; f < topologyNodes.size();f++){
        std::string s2 = "";
        for(uint32_t j = 0; j <queryOperators.size(); j++){
            if(initialPlacement.chromosome.at(f*queryOperators.size()+j)){
                s2.append(std::to_string(queryOperators[j]->as<OperatorNode>()->getId()));
                s2.append(" ");
            }
        }
        NES_DEBUG("Topology Node with ID:  "<<topologyNodes[f]->getId()<<" has Operators:  "<<s2);
    }
    NES_DEBUG("Optimized Placement: ");
    // print the placement
    for(uint32_t f = 0 ; f < topologyNodes.size();f++){
        std::string s2 = "";
        for(uint32_t j = 0; j <queryOperators.size(); j++){

            if(optimizedPlacement.chromosome.at(f*queryOperators.size()+j)){
                s2.append(std::to_string(queryOperators[j]->as<OperatorNode>()->getId()));
                s2.append(" ");
            }
        }
        NES_DEBUG("Topology Node with ID:  "<<topologyNodes[f]->getId()<<" has Operators:  "<<s2);
    }
    NES_DEBUG("Initial Cost: " << initialPlacement.cost);
    NES_DEBUG("Optimized Cost: " << optimizedPlacement.cost);

    PlacementMatrix mapping;
    for (uint32_t i = 0; i < topologyNodes.size(); i++) {
        std::vector<bool> genome;
        auto assignmentBegin = optimizedPlacement.chromosome.begin() + i * queryOperators.size();
        genome.assign(assignmentBegin, assignmentBegin + queryOperators.size());
        mapping.push_back(genome);
    }

    assignMappingToTopology(topology, queryPlan, mapping);
}

void GeneticAlgorithmStrategy::initializePopulation(QueryPlanPtr queryPlan, uint32_t populationSize) {

    for (uint32_t i = 0; i < populationSize; i++) {
        Placement placement = generatePlacement(queryPlan);
        // check if an identical placement already exists to avoid duplicate placements.
        if(placementAlreadyExists(population,placement))
            continue;
        placement.cost = getCost(placement);
        // Find the index of the current placement in population depending on the cost, we order the placements from low to high cost
        auto pos = std::find_if(population.begin(), population.end(), [placement](auto s) {
            return s.cost > placement.cost;
        });
        population.insert(pos, placement);
    }
}

GeneticAlgorithmStrategy::Placement GeneticAlgorithmStrategy::generatePlacement(QueryPlanPtr queryPlan) {
    Placement placement;
    uint32_t numOfTopologyNodes = topologyNodes.size();
    uint32_t numOfOperators = queryOperators.size();
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
        //Get the vector of topology source nodes of the current operator
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
        }    //  if the current topology node is not a source node
        if (!visitedQueryOperators.at(operatorIndex)) {
            OperatorNodePtr parentOperator = queryOperators.at(operatorIndex)
                                                 ->getParents()[0]
                                                 ->as<OperatorNode>();//We assume each operator to have only one parent
            auto it = std::find(queryOperators.begin(), queryOperators.end(), parentOperator);
            int parentOperatorIndex = std::distance(queryOperators.begin(), it);
            int parentOperatorTopologyIndex = -1;//topology node where the parent operator is assigned
            for (uint32_t i = 0; i < topologyNodes.size(); i++) {
                if (chromosome[i * numOfOperators + parentOperatorIndex]) {
                    parentOperatorTopologyIndex = i;
                    break;
                }
            }
            visitedQueryOperators.at(operatorIndex) = true;
            candidateTopologyNodeIndex = parentOperatorTopologyIndex;
        }

        int placeTheOperator = distrib(gen);    //randomly decide whether to place the operator in the current topology node
        if (!placeTheOperator) {
            int newCandidateNodeIndex = -1;
            auto children = topologyNodes[candidateTopologyNodeIndex]->getChildren();   // get children topology nodes of the current topology node
            // find the child node that is reachable from all source nodes of the operator, since we assume a tree-like topology there will be at most only one valid child node
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
            // if none of the child nodes is valid, then place the operator in the parent node
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
GeneticAlgorithmStrategy::getOptimizedPlacement(std::vector<Placement> population, uint32_t numOfIterations, uint32_t patience, double threshold, uint32_t numOfGenesToMutate, double mutationProbability,QueryPlanPtr queryPlan) {

    Placement optimizedPlacement = population[0];   //keep track of the best placement so far.
    std::vector<GeneticAlgorithmStrategy::Placement> offspringPopulation;
    uint32_t populationSize = population.size();
    std::vector<double> optimizedCostOfEachIteration;
    double initialCost = population[0].cost;
    /*std::map<uint32_t, uint32_t> populationSizes;
    std::map<uint32_t, double> costsAfterGA;
    std::map<uint32_t, int> counts;
    NES_DEBUG("Cost before Optimization:  " << population[0].cost);
    costsAfterGA.insert(std::make_pair(0, optimizedPlacement.cost));    */
    uint32_t  numOfIterationsWithoutImprovement = 0;
    for (uint32_t i = 0; i < numOfIterations; i++) {
        /* int i = 0;
    for (auto start = std::chrono::steady_clock::now(), now = start; now < start + std::chrono::seconds{time};
         now = std::chrono::steady_clock::now()) {
        int numOfInvalidOffsprings = 0; */

        uint32_t j = 0;
        while ( j < population.size() -2) {
            if(offspringPopulation.size() >= populationSize)
                break;
            Placement parent1 = population[j];
            for (uint32_t k = j+1; k < population.size() -1; k++) {
                Placement parent2 = population[k];
                //srand(time(NULL));
                //uint32_t crossOverIndex = (uint32_t) (1 + rand() % (queryOperators.size()-2));
                uint32_t crossOverIndex = queryOperators.size()/2;  // We cross over the query in the middle
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
                    //numOfInvalidOffsprings += 1;
                }
                if (checkPlacementValidation(tempOffspring2, queryPlan)) {
                    tempOffspring2.cost = getCost(tempOffspring2);
                }
                else{
                    //numOfInvalidOffsprings += 1;
                }
                offspring1 = mutate(offspring1, queryPlan, numOfGenesToMutate, mutationProbability);
                offspring2 = mutate(offspring2, queryPlan, numOfGenesToMutate, mutationProbability);
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
                if(offspringPopulation.size() >= populationSize)
                    break;
                if (!placementAlreadyExists(offspringPopulation, offspring2)) {
                    auto pos = std::find_if(offspringPopulation.begin(), offspringPopulation.end(), [offspring2](auto s) {
                        return s.cost > offspring2.cost;
                    });
                    offspringPopulation.insert(pos, offspring2);
                }
                if(offspringPopulation.size() >= populationSize)
                    break;
            }
            j += 1;
        }
        numOfIterationsWithoutImprovement += 1;
        /*populationSizes.insert(std::make_pair(i, selectedPortion));
        counts.insert(std::make_pair(i, numOfInvalidOffsprings));
        costsAfterGA.insert(std::make_pair(i + 1, offspringPopulation[0].cost));  */
        optimizedCostOfEachIteration.push_back(offspringPopulation[0].cost);
        double change = getAverageRelativeChangeOfCostOverIterations(optimizedCostOfEachIteration, initialCost);
        if (offspringPopulation.size() && optimizedPlacement.cost > offspringPopulation[0].cost) {
            optimizedPlacement = offspringPopulation[0];
            numOfIterationsWithoutImprovement = 0;
        }
        if(numOfIterationsWithoutImprovement >= patience && change < threshold){
            return optimizedPlacement;
        }
        //i += 1;
        population.clear();
        population = offspringPopulation;
        offspringPopulation.clear();
    }
    /*
    NES_DEBUG("Cost Before Optimization:  " << costsAfterGA[0]);
    for (auto& [i, numOfInvalidOffsprings] : counts) {
        NES_DEBUG("Current Generation:  " << i+1);
        NES_DEBUG("Optimized Cost of this Generation:  " << costsAfterGA[i+1]);
        NES_DEBUG("Current Population Size:  " << populationSizes.at(i));
        NES_DEBUG("Number of invalid offsprings:  " << numOfInvalidOffsprings);
        //NES_DEBUG("Best cost of this generation:  " << costs[i]);

    }   */

    return optimizedPlacement;
}

GeneticAlgorithmStrategy::Placement GeneticAlgorithmStrategy::mutate(GeneticAlgorithmStrategy::Placement placement,
                                                                     QueryPlanPtr queryPlan, uint32_t numOfGenesToMutate,  double mutationProbability) {
    Placement mutatedPlacement = placement;
    auto sourceOperators = queryPlan->getSourceOperators();
    // select random index of gen to mutate
    std::vector<uint32_t> randomMutationIndex;
    srand(time(0));
    for(uint32_t i = 0; i < numOfGenesToMutate; i++) {
        double r = static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
        uint32_t mutationOperatorIdx = (uint32_t) RAND_MAX;
        if (r < mutationProbability) {
            mutationOperatorIdx = rand() % queryOperators.size();
            uint32_t attempt = 0;  // We try 3 times to get an operator other than a source operator
            while (attempt < 3 && (mutationOperatorIdx >= (uint32_t) RAND_MAX
                   || (find(sourceOperators.begin(), sourceOperators.end(), queryOperators.at(mutationOperatorIdx))
                       != sourceOperators.end()))) {
                mutationOperatorIdx = rand() % queryOperators.size();
            }
            if(mutationOperatorIdx >= (uint32_t) RAND_MAX)
                return placement;
            //find the node where the operator to mutate is placed in
            uint32_t mutationNodeIdx = (uint32_t) RAND_MAX;
            for (uint32_t i = 0; i < topologyNodes.size(); i++) {
                if (placement.chromosome.at(i * queryOperators.size() + mutationOperatorIdx)) {
                    mutationNodeIdx = i;
                    break;
                }
            }
            if (mutationNodeIdx == RAND_MAX)
                return placement;
            //move the operator from the old node to the new one
            uint32_t newMutationNodeIdx = rand() % topologyNodes.size();
            uint32_t temp = mutationNodeIdx * queryOperators.size() + mutationOperatorIdx;
            mutatedPlacement.chromosome.at(temp) = false;
            mutatedPlacement.chromosome.at(newMutationNodeIdx * queryOperators.size() + mutationOperatorIdx) = true;
        }
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
    std::map<int, std::set<int>> mapOfOperatorToSourceOperatorId;
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
        // create a vector of all operators that process data from this source operator
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
            mapOfOperatorToSourceOperatorId[index].insert(sourceOperator->getId());
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
    std::map<uint32_t , double> globalNodesCost;    //global cost of nodes in the placement
    // we calculate the global costs of the nodes at the bottom of the topology and move upwards, we exclude the root node from the cost
    for (int i = mapOfBreadthFirstToDepthFirst.size() - 1; i >0; i--) {
        uint32_t nodeIndex = mapOfBreadthFirstToDepthFirst[i];   // when generating the placement we use depth first iteration, when calculating the cost we use depth first
        uint32_t assignment_begin = nodeIndex * numberOfOperators;
        double localCost = 0.0;    //if no operators are assigned to the  current node then the local cost is 1
        int nodeLoad = 0;
        std::map<int, std::vector<double>> mapOfSourceNodeToDMFs; // map of source node to the DMFs of its stream operators (we group the operators placed in the current node based on their physical streams)
        //check which operators are assigned to the current node and add the cost of these operators to the local cost of the current node
        for (int j = 0; j < numberOfOperators; j++) {
            if (placement.chromosome[assignment_begin+j]) {
                auto sourcesNodesOfTheCurrentOperator = mapOfOperatorToSourceNodes[j];
                for(auto sourcesNodeOfTheCurrentOperator : sourcesNodesOfTheCurrentOperator){
                    mapOfSourceNodeToDMFs[sourcesNodeOfTheCurrentOperator].push_back(operatorDMF.at(j));
                }
                nodeLoad += operatorLoad.at(j);
            }
        }
        //For each physical stream, we multiply the DMFs of its operators then we sum up the results
        for(auto physicalStreamDMFs : mapOfSourceNodeToDMFs){
            double streamCost = 1.0;    // In case the none of the operators of this physical stream is placed in the current node (avoid multiplying by zero)
            for(auto streamDMF : physicalStreamDMFs.second){
                streamCost *= streamDMF;
            }
            localCost += streamCost;
        }

        //check whether the current node is overloaded if yes then add the overloading penalization the local cost
        auto availableCapacity = topologyNodes[nodeIndex]->getAvailableResources();
        if (nodeLoad > availableCapacity){
            localCost += (double)(nodeLoad - availableCapacity);
        }

        //Add the global cost of the children nodes to the global cost of the current node
        double childGlobalCost = 0;
        double globalCost = 0;
        auto children = topologyNodes.at(nodeIndex)->getChildren();
        for (auto& child : children) {
            childGlobalCost = globalNodesCost.at(child->as<TopologyNode>()->getId());
            globalCost += localCost * childGlobalCost;
        }
        //In case the current node has no children then the global cost is the local cost
        if (globalCost == 0){
            globalCost = localCost;
        }
        globalNodesCost.insert(std::make_pair(topologyNodes[nodeIndex]->getId(), globalCost));
    }
    double cost = 0;
    std::map<uint32_t , double>::iterator it;
    for (it = globalNodesCost.begin(); it != globalNodesCost.end(); it++) {
        cost += it->second;
    }
    return cost;
}

GeneticAlgorithmStrategy::Placement GeneticAlgorithmStrategy::crossOver(GeneticAlgorithmStrategy::Placement placement,
                                                                        GeneticAlgorithmStrategy::Placement other, uint32_t crossOverIndex) {

    Placement offspring;
    std::vector<bool> offspringChromosome;
    // for each node we cross over the query from both parents.
    for (uint32_t i = 0; i < topologyNodes.size(); i++) {
        std::vector<bool> genome;       //operator assignment to the current node
        std::vector<bool> genomePart1;  //from parent 1
        std::vector<bool> genomePart2;  //from parent 2
        auto genomPart1Beginn = placement.chromosome.begin() + i * queryOperators.size();
        auto genomPart2Beginn = other.chromosome.begin()+crossOverIndex + i * queryOperators.size();
        genomePart1.assign(genomPart1Beginn, genomPart1Beginn + crossOverIndex);
        genomePart2.assign(genomPart2Beginn, genomPart2Beginn + (queryOperators.size()-crossOverIndex));
        genome.insert(genome.end(),genomePart1.begin(),genomePart1.end());
        genome.insert(genome.end(),genomePart2.begin(),genomePart2.end());
        offspringChromosome.insert(offspringChromosome.end(),genome.begin(),genome.end());
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
std::vector<uint32_t> GeneticAlgorithmStrategy::breadthFirstNodeIterator(TopologyPtr topology) {

    std::vector<uint32_t> mapOfBreadthFirstToDeepFirst;
    auto bfIterator = BreadthFirstNodeIterator(topology->getRoot());
    for (auto itr = bfIterator.begin(); itr != bfIterator.end(); ++itr) {
        for(uint32_t i = 0; i < topologyNodes.size(); i++){
            if((*itr)->as<TopologyNode>()->getId() == topologyNodes[i]->getId()){
                mapOfBreadthFirstToDeepFirst.push_back(i);
                break;
            }
        }
    }
    return mapOfBreadthFirstToDeepFirst;
}
double GeneticAlgorithmStrategy::getAverageRelativeChangeOfCostOverIterations(std::vector<double>optimizedCostOfEachIteration, double initialCost){
    double result = 0.0;
    for(auto cost : optimizedCostOfEachIteration){
        result += cost;
    }
    result = result/optimizedCostOfEachIteration.size();
    result = 100.0*(result - initialCost)/initialCost;
    return result;
}
}// namespace NES::Optimizer

