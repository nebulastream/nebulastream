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
    Placement optimizedPlacement = getOptimizedPlacement(population, 10, queryPlan);

   /* for (uint32_t f = 0; f < queryOperators.size(); f++) {
        for (uint32_t j = 0; j < topologyNodes.size(); j++) {
            if (optimizedPlacement.chromosome.at(j * queryOperators.size() + f))
                NES_DEBUG("Operator with ID: " << queryOperators[f]->as<LogicalOperatorNode>()->getId() << " is assigned to Node with ID: "
                                               << topologyNodes[j]->getId());
        }
    }
    NES_DEBUG("Optimized Placement Topology: ");
    optimizedPlacement.topology->print();   */
    //Placement optimizedPlacement = population[0];
    /*
    Placement bestInitialPlacement = population[0];
    std::list<int> listOfInts( {1,2,3,4,5,6,7,8,9,10});
    std::map<int, std::vector<double>> counts;
    for(auto time: listOfInts){
        std::vector<double> costs;

        for(int i = 0; i < 3; i++){
            Placement optimizedPlacement = getOptimizedPlacement(population,time, queryPlan);
            if(optimizedPlacement.cost < bestInitialPlacement.cost)
                costs.push_back(bestInitialPlacement.cost - optimizedPlacement.cost);
            else
                costs.push_back(0.0);
        }
        counts.insert(std::make_pair(time, costs));
    }
    for (auto& [n, counts_n] : counts) {
        std::sort(counts_n.begin(), counts_n.end());
        long median = counts_n[counts_n.size() / 2];
        if (counts.size() % 2 == 0) {
            median = (median + counts_n[(counts_n.size() - 1) / 2]) / 2;
        }
        std::stringstream ss;
        for (auto& count : counts_n) {
            ss << count << ", ";
        }

        NES_INFO("N: " << n << ", median: " << median << ", measures: " << ss.str());
    }
    //
    initializePopulation(queryPlan);
    //Placement bestInitialPlacement = population[0];
   Placement optimizedPlacement = getOptimizedPlacement(population,0, queryPlan);
   if(optimizedPlacement.cost < bestInitialPlacement.cost){
       NES_DEBUG("GA has optimized the placement ");
       NES_DEBUG("Best Initial cost: "<< bestInitialPlacement.cost);
       NES_DEBUG("Optimized cost: "<<optimizedPlacement.cost);
        NES_DEBUG("Best Initial Placement: ");
        // print the placement
        for(uint32_t f = 0 ; f < queryOperators.size();f++){
            for(uint32_t j = 0; j <topologyNodes.size(); j++){
                if(bestInitialPlacement.chromosome.at(j*queryOperators.size()+f))
                    NES_DEBUG("Operator "<<f<<" is assigned to Node "<<j);
            }
        }
        NES_DEBUG("Optimized Placement: ");
        // print the placement
        for(uint32_t f = 0 ; f < queryOperators.size();f++){
            for(uint32_t j = 0; j <topologyNodes.size(); j++){
                if(optimizedPlacement.chromosome.at(j*queryOperators.size()+f))
                    NES_DEBUG("Operator "<<f<<" is assigned to Node "<<j);
            }
        }
    }
    */

    PlacementMatrix mapping;
    for(uint32_t i = 0; i < topologyNodes.size(); i++){
        std::vector<bool> genome;
        auto assignmentBegin = optimizedPlacement.chromosome.begin() + i * queryOperators.size();
        genome.assign(assignmentBegin, assignmentBegin + queryOperators.size());
        mapping.push_back(genome);
    }

    assignMappingToTopology(topology, queryPlan, mapping);
    optimizedPlacement.topology->print();
}

void GeneticAlgorithmStrategy::initializePopulation(QueryPlanPtr queryPlan) {

    for (int i = 0; i < 100; i++) {
        Placement placement = generatePlacement(queryPlan);
        placement.cost = getCost(placement);
        auto pos = std::find_if(population.begin(), population.end(), [placement](auto s) {
            return s.cost > placement.cost;
        });
        population.insert(pos, placement);
      /*  bool valid = checkPlacementValidation(placement, queryPlan);
        if (!valid) {
            NES_DEBUG("Placement is invalid ");
            exit(1);
        }   */
    }
}

GeneticAlgorithmStrategy::Placement GeneticAlgorithmStrategy::generatePlacement(QueryPlanPtr queryPlan) {
    Placement placement;
    uint32_t numOfTopologyNodes = topologyNodes.size();
    numOfOperators = queryOperators.size();
    std::vector<bool> chromosome(numOfOperators * numOfTopologyNodes, false);
    //std::vector<bool> chromosome(numOfOperators * numOfTopologyNodes);
    TopologyPtr topologyOfPlacement = Topology::create();
    TopologyNodePtr root = topologyNodes.at(0)->copy();//Set the root of topology as root of the candidate placement
    topologyOfPlacement->setAsRoot(root);
    std::vector<int> candidateTopologyNodesIndices = {0};//the position of candidate nodes in "topologyNodes" vector
    uint32_t operatorIndex = 1;                          // start from the second operator because the first operator is sink
    chromosome[0] = true;                                //place the sink operator in the root node
    uint32_t numOfOperatorsToPlace = queryOperators.size() - queryPlan->getSinkOperators().size();
    std::vector<bool> visitedQueryOperators(queryOperators.size());
    visitedQueryOperators.at(0) = true;//sink operator is already placed
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    srand(time(0));
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, 1);
    while (numOfOperatorsToPlace) {
        if (!visitedQueryOperators.at(operatorIndex)) {
            OperatorNodePtr parentOperator = queryOperators.at(operatorIndex)
                                                 ->getParents()[0]
                                                 ->as<OperatorNode>();//We assume each operator to have only one parent
            auto it = std::find(queryOperators.begin(), queryOperators.end(), parentOperator);
            int parentOperatorIndex = std::distance(queryOperators.begin(), it);
            std::vector<int> parentOperatorTopologyIndices;//topology nodes where the parent operator is assigned
            for (uint32_t i = 0; i < topologyNodes.size(); i++) {
                if (chromosome.at(i * queryOperators.size() + parentOperatorIndex))
                    parentOperatorTopologyIndices.push_back(i);
            }
            visitedQueryOperators.at(operatorIndex) = true;
            candidateTopologyNodesIndices = parentOperatorTopologyIndices;
        }

        int placeTheOperator = distrib(gen);
        // Check if the current operator to place is a source operator
        if (std::find(sourceOperators.begin(), sourceOperators.end(), queryOperators.at(operatorIndex)) != sourceOperators.end())
            placeTheOperator = 0;
        if (!placeTheOperator) {
            std::vector<int> newCandidateNodesIndices;
            for (auto candidateTopologyNodesIndex : candidateTopologyNodesIndices) {
                auto children = topologyNodes.at(candidateTopologyNodesIndex)->getChildren();
                for (auto child : children) {
                    auto iterator = std::find(topologyNodes.begin() + candidateTopologyNodesIndex, topologyNodes.end(), child);
                    int childNodeIndex = std::distance(topologyNodes.begin(), iterator);
                    if (find(newCandidateNodesIndices.begin(), newCandidateNodesIndices.end(), childNodeIndex)
                        == newCandidateNodesIndices.end())
                        newCandidateNodesIndices.push_back(childNodeIndex);
                }
            }

            std::vector<int> operatorSourceNodeIndices = mapOfOperatorToSourceNodes.find(operatorIndex)->second;

            //Get the candidate nodes which are not reachable from all operator source nodes
            std::vector<int> nodesToDelete;
            for (uint32_t i = 0; i < newCandidateNodesIndices.size(); i++) {
                //bool reachable = true;
                for (auto operatorSourceNodeIndex : operatorSourceNodeIndices) {
                    if (!transitiveClosureOfTopology.at(newCandidateNodesIndices.at(i) * numOfTopologyNodes
                                                        + operatorSourceNodeIndex)) {
                        nodesToDelete.push_back(i);
                        break;
                    }
                }
            }
            while (nodesToDelete.size()) {
                int idx = nodesToDelete.back();
                newCandidateNodesIndices.erase(newCandidateNodesIndices.begin() + idx);
                nodesToDelete.pop_back();
            }
            //Delete candidate nodes that can reach other candidate nodes
            eliminateReachableNodes(&newCandidateNodesIndices);
            if (newCandidateNodesIndices.empty()) {
                int topologyNodeIdx = rand() % candidateTopologyNodesIndices.size();
                chromosome.at(candidateTopologyNodesIndices.at(topologyNodeIdx) * numOfOperators + operatorIndex) = true;
                operatorIndex += 1;
                numOfOperatorsToPlace -= 1;
                continue;
            }
            int selectedTopologyNodesIndex = newCandidateNodesIndices[rand() % newCandidateNodesIndices.size()];

            uint32_t maxIndex = operatorIndex;

            for (int candidateTopologyNodesIndex : candidateTopologyNodesIndices) {

                if (topologyNodes.at(candidateTopologyNodesIndex)->containAsChild(topologyNodes.at(selectedTopologyNodesIndex))) {
                    int nodeId = topologyNodes.at(candidateTopologyNodesIndex)->getId();
                    auto parent = topologyOfPlacement->findNodeWithId(nodeId);
                    auto child = topologyNodes.at(selectedTopologyNodesIndex)->copy();
                    auto childNodeIsAdded = topologyOfPlacement->findNodeWithId(child->getId());
                    if (childNodeIsAdded == nullptr) {
                        topologyOfPlacement->addNewPhysicalNodeAsChild(parent, child);
                    }
                    LinkPropertyPtr linkProperty = topologyNodes.at(candidateTopologyNodesIndex)
                                                       ->getLinkProperty(topologyNodes.at(selectedTopologyNodesIndex));
                    parent->addLinkProperty(child, linkProperty);
                    child->addLinkProperty(parent, linkProperty);

                    auto iterator =
                        std::find(operatorSourceNodeIndices.begin(), operatorSourceNodeIndices.end(), selectedTopologyNodesIndex);
                    if (iterator != operatorSourceNodeIndices.end()) {
                        int sourceNodeIndex = *iterator;
                        for (uint32_t m = operatorIndex; m < queryOperators.size(); m++) {
                            auto operatorToPlace = queryOperators.at(m);
                            auto operatorSourceNodesIndices = mapOfOperatorToSourceNodes.find(m)->second;
                            auto itr = std::find(operatorSourceNodesIndices.begin(),
                                                 operatorSourceNodesIndices.end(),
                                                 selectedTopologyNodesIndex);
                            if (itr != operatorSourceNodesIndices.end()) {
                                chromosome.at(sourceNodeIndex * queryOperators.size() + m) = true;

                                if (m >= maxIndex) {
                                    maxIndex = m + 1;
                                }
                            }
                        }
                    }
                }
            }

            if (maxIndex >= operatorIndex) {
                operatorIndex = maxIndex;
                numOfOperatorsToPlace = numOfOperators - operatorIndex;
            }

            candidateTopologyNodesIndices.clear();
            candidateTopologyNodesIndices.push_back(selectedTopologyNodesIndex);
        }// If placeTheOperator is false
        else {
            for (auto topologyNodeIndex : candidateTopologyNodesIndices) {
                chromosome.at(topologyNodeIndex * numOfOperators + operatorIndex) = true;
            }
            operatorIndex += 1;
            numOfOperatorsToPlace -= 1;
        }
    }
    placement.topology = topologyOfPlacement;
    placement.chromosome = chromosome;
    return placement;
}

GeneticAlgorithmStrategy::Placement
GeneticAlgorithmStrategy::getOptimizedPlacement(std::vector<Placement> population, int time, QueryPlanPtr queryPlan) {

    Placement optimizedPlacement = population[0];
    //std::cout <<"numberOfGeneration:" <<numberOfGeneration;
    std::vector<GeneticAlgorithmStrategy::Placement> offspringPopulation;
    std::map<uint32_t, int> counts;
    std::map<uint32_t, double> costsAfterGA;
    std::vector<double> costs;
   // NES_DEBUG("Cost before Optimization:  " << population[0].cost);
    costsAfterGA.insert(std::make_pair(0, optimizedPlacement.cost));
    for (int i = 0; i < time; i++) {
   /* int i = 0;
    for (auto start = std::chrono::steady_clock::now(), now = start; now < start + std::chrono::seconds{time};
         now = std::chrono::steady_clock::now()) {
        int numOfInvalidOffsprings = 0; */
        // cross over placements from the current generation
  // int numOfInvalidOffsprings = 0;
   uint32_t selectedPortion = 10;
   if (population.size() < selectedPortion)
       selectedPortion = population.size();

        for (uint32_t j = 0; j < selectedPortion; j++) {
            for (uint32_t k = 0; k < selectedPortion; k++) {
                if (j == k)
                    continue;
                Placement parent1 = population[j];
                Placement parent2 = population[k];

                Placement currentOffspring = crossOver(parent1, parent2);
               // currentOffspring.topology->print();
                Placement temp = currentOffspring;
                temp.cost = (double) RAND_MAX;
                if (checkPlacementValidation(temp, queryPlan))
                   temp.cost = getCost(temp);
                currentOffspring = mutate(currentOffspring, queryPlan);
                if (checkPlacementValidation(currentOffspring, queryPlan)) {

                    currentOffspring.cost = getCost(currentOffspring);
                    if ((currentOffspring.cost < parent1.cost) && (currentOffspring.cost < parent2.cost)) {
                        NES_DEBUG("Crossover found a better solution! ");
                    }
                } else {
                    if(temp.cost >= (double) RAND_MAX){
                        //numOfInvalidOffsprings += 1;
                        currentOffspring.cost = (double) RAND_MAX;
                    }

                }
                if (temp.cost < currentOffspring.cost)
                    currentOffspring = temp;
                if (!offspingExists(offspringPopulation, currentOffspring)) {
                    auto pos = std::find_if(offspringPopulation.begin(), offspringPopulation.end(), [currentOffspring](auto s) {
                        return s.cost > currentOffspring.cost;
                    });
                    offspringPopulation.insert(pos, currentOffspring);
                }
            }
        }
        /*
        select.insert(std::make_pair(i, selectedPortion));
        counts.insert(std::make_pair(i, numOfInvalidOffsprings));
        costs.push_back(offspringPopulation[0].cost);   */
        costsAfterGA.insert(std::make_pair(i+1, offspringPopulation[0].cost));

        if (optimizedPlacement.cost > offspringPopulation[0].cost) {
            optimizedPlacement = offspringPopulation[0];
        }
        /*else if(optimizedPlacement.cost < offspringPopulation[0].cost){
            offspringPopulation.insert(offspringPopulation.begin(),optimizedPlacement);
            NES_DEBUG("we inserted optimized Placement to offspring Generation with cost:  "<< optimizedPlacement.cost);
            NES_DEBUG("Cost of best placement in this generation:  "<< offspringPopulation[0].cost);
        }*/
        //i += 1;
        population = offspringPopulation;
        offspringPopulation.clear();
    }
    for (auto& [i, cost] : costsAfterGA) {
        NES_DEBUG("Current Generation:  " << i);
        NES_DEBUG("Cost of Current Population:  " << cost);
    }
/*
    int totalNumOfInvalidOffsprings = 0;
    for (auto& [i, numOfInvalidOffsprings] : counts) {
        NES_DEBUG("Current Generation:  " << i+1);
        NES_DEBUG("Current Population Size:  " << select.at(i));
        NES_DEBUG("Number of invalid offsprings:  " << numOfInvalidOffsprings);
        //NES_DEBUG("Best cost of this generation:  " << costs[i]);
        totalNumOfInvalidOffsprings += numOfInvalidOffsprings;
    }

    NES_DEBUG("numOfInvalidOffsprings:  " << totalNumOfInvalidOffsprings);  */
    return optimizedPlacement;
}

GeneticAlgorithmStrategy::Placement GeneticAlgorithmStrategy::mutate(GeneticAlgorithmStrategy::Placement placement,
                                                                     QueryPlanPtr queryPlan) {
    Placement mutatedPlacement = placement;
    uint32_t numberOfMutatedGen = 2;
    double mutationProbability = 0.0001;
    auto sourceOperators = queryPlan->getSourceOperators();
    // select random index of gen to mutate
    std::vector<uint32_t> randomMutationIndex;
    srand(time(0));
    for (uint32_t i = 0; i < numberOfMutatedGen; i++) {
        double r = static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
        if (r < mutationProbability) {
            uint32_t mutationNodeIdx = rand() % topologyNodes.size();
            uint32_t mutationOperatorIdx = rand() % queryOperators.size();
            while (!mutationOperatorIdx
                   || (find(sourceOperators.begin(), sourceOperators.end(), queryOperators.at(mutationOperatorIdx))
                       != sourceOperators.end())) {
                mutationOperatorIdx = rand() % queryOperators.size();
            }
            uint32_t mutationIdx = mutationNodeIdx * queryOperators.size() + mutationOperatorIdx;
            NES_DEBUG("GeneticAlgorithmOptimizer: mutationIdx=" << mutationIdx);
            bool originalValue = placement.chromosome.at(mutationIdx);
            mutatedPlacement.chromosome.at(mutationIdx) = !originalValue;
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

    /*  //print the transitive closure of topology
     for (int i = 0; i < numOfTopologyNodes; i++) {
        for (int j = 0; j < numOfTopologyNodes; j++)
            std::cout << transitiveClosureOfTopology.at(i * numOfTopologyNodes + j) << " ";
        std::cout << std::endl;
    }*/

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
        std::vector<int> currentOperatorsNodes;
        std::vector<int> childrenOperatorsPositions;
        auto childrenOperators = queryOperators[i]->getChildren();

        //Get the indices of the children operators
        for (auto child : childrenOperators) {
            auto it = std::find(queryOperators.begin(), queryOperators.end(), child);
            int pos = std::distance(queryOperators.begin(), it);
            childrenOperatorsPositions.push_back(pos);
        }

        //Get the topology nodes where the current operator is assigned to
        for (int j = 0; j < numOfTopologyNodes; j++) {
            if (chromosome[(j * queryOperators.size()) + i])
                currentOperatorsNodes.push_back(j);
        }
        //Check if the current operator is assigned to nodes reachable from each other
        uint32_t numOfCurrentOperatorNodes = currentOperatorsNodes.size();
        eliminateReachableNodes(&currentOperatorsNodes);
        if (numOfCurrentOperatorNodes != currentOperatorsNodes.size())
            return false;
        //Check if the current query operator is assigned to topology nodes
        if (currentOperatorsNodes.empty())
            return false;
        //Get the topology nodes where the children query operators are assigned to
        for (int childIndex : childrenOperatorsPositions) {
            for (int j = 0; j < numOfTopologyNodes; j++)
                if (chromosome[(j * queryOperators.size()) + childIndex])
                    childrenOperatorsNodesIndices.push_back(j);
        }

        //Check if each topology node of children operators can reach at least one topology node of current query operator
        bool childrenOperatorsPlacement;
        for (int childOperatorNodeIdx : childrenOperatorsNodesIndices) {
            childrenOperatorsPlacement = false;
            for (int currentNode : currentOperatorsNodes) {
                if (transitiveClosureOfTopology.at(currentNode * numOfTopologyNodes + childOperatorNodeIdx)) {
                    childrenOperatorsPlacement = true;
                    break;
                }
            }
            if (!childrenOperatorsPlacement && !childrenOperatorsNodesIndices.empty())
                return false;
        }

        //Check if each topology node of the current  operator is reachable at least from one topology node of the children query operators
        for (int currentNode : currentOperatorsNodes) {
            bool currentOperatorPlacement = false;
            for (int childNode : childrenOperatorsNodesIndices) {
                if (transitiveClosureOfTopology.at(currentNode * numOfTopologyNodes + childNode)) {
                    currentOperatorPlacement = true;
                    break;
                }
            }
            if (!currentOperatorPlacement && childrenOperatorsNodesIndices.size())
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

void GeneticAlgorithmStrategy::eliminateReachableNodes(std::vector<int>* topologyIndices) {
    std::vector<int> nodesToDelete;
    for (uint32_t i = 0; i < topologyIndices->size(); i++) {
        for (uint32_t j = 0; j < topologyIndices->size(); j++) {
            if (i == j)
                continue;
            if (transitiveClosureOfTopology.at(topologyIndices->at(i) * topologyNodes.size() + topologyIndices->at(j))) {
                if (std::find(nodesToDelete.begin(), nodesToDelete.end(), j) == nodesToDelete.end()) {
                    nodesToDelete.push_back(j);
                }
            }
        }
    }

    while (nodesToDelete.size()) {
        int idx = nodesToDelete.back();
        topologyIndices->erase(topologyIndices->begin() + idx);
        nodesToDelete.pop_back();
    }
}

double GeneticAlgorithmStrategy::getCost(Placement placement) {
    int numberOfOperators = queryOperators.size();
    //global cost of nodes in the placement
    std::map<NodePtr, double> globalNodesCost;
    std::vector<TopologyNodePtr> placementTopologyNodes = topologySnapshot(placement.topology);

    //Iterate over all nodes in the placement to calculate the global cost of each node
    for (int i = placementTopologyNodes.size() - 1; i >= 0; i--) {
        int assignment_begin = i * numberOfOperators;
        double localCost = 1;//if no operators are assigned to the  current node then the local cost is 1
        int nodeLoad = 0;

        //check which operators are assigned to the current node and add the cost of these operators to the local cost of the current node
        for (int j = 0; j < numberOfOperators; j++) {
            if (placement.chromosome[assignment_begin + j]) {
                localCost *= operatorDMF.at(j);
                nodeLoad += operatorLoad.at(j);
            }
        }

        //check wether the current node is overloaded if yes then add the overloading penalization  the local cost
        auto availableCapacity = placementTopologyNodes.at(i)->getAvailableResources();
        if (nodeLoad > availableCapacity)
            localCost += (nodeLoad - availableCapacity);

        //Add the global cost of the children nodes to the global cost of the current node
        double childGlobalCost = 0;
        double globalCost = 0;
        auto children = placementTopologyNodes.at(i)->getChildren();
        for (auto& child : children) {
            childGlobalCost = globalNodesCost.at(child);
            /* // Penalize link overloading and add it to the global cost
            auto linkCapacity = placementTopologyNodes.at(i)->getLinkProperty(child->as<TopologyNode>())->bandwidth;
            if (childGlobalCost > linkCapacity)
                globalCost += childGlobalCost - linkCapacity;   */

            globalCost += localCost * childGlobalCost;
        }
        //In case the current node has no children then the global cost is the local cost
        if (globalCost == 0)
            globalCost = localCost;
        globalNodesCost.insert(std::make_pair(placementTopologyNodes.at(i), globalCost));
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
                                                                        GeneticAlgorithmStrategy::Placement other) {

    other.cost = getCost(other);
    Placement offspring = placement;
    TopologyPtr offspringTopology = Topology::create();
    TopologyNodePtr rootNode = placement.topology->getRoot()->copy();
    offspringTopology->setAsRoot(rootNode);
    std::vector<TopologyNodePtr> placementTopologyNodes = topologySnapshot(placement.topology);
    std::vector<TopologyNodePtr> otherTopologyNodes = topologySnapshot(other.topology);

    std::vector<bool> offspringChromosome;
    std::vector<TopologyNodePtr> offspringTopologyNodes;
    uint32_t startPlacement = 0;
    uint32_t endPlacement = 0;
    uint32_t startOther = 0;
    // uint32_t endOther = 0;

    for (uint32_t j = 0; j < topologyNodes.size(); j++) {
        if (placement.chromosome.at(j * queryOperators.size() + 1)) {
            startPlacement = j * queryOperators.size() + 1;
            break;
        }
    }
    for (uint32_t j = 0; j < topologyNodes.size(); j++) {
        if (placement.chromosome.at(j * queryOperators.size() + (queryOperators.size() - 2))) {
            endPlacement = j * queryOperators.size() + (queryOperators.size() - 2);
            break;
        }
    }
    for (uint32_t j = 0; j < topologyNodes.size(); j++) {
        if (other.chromosome.at(j * queryOperators.size() + 1)) {
            startOther = j * queryOperators.size() + 1;
            break;
        }
    }
    uint32_t crossOverIndex = 0;
    srand(time(NULL));
    if (startPlacement == endPlacement)
        crossOverIndex = (uint32_t) (rand() % (placement.chromosome.size() - startPlacement)) + startPlacement;
    else {
        if (startOther > startPlacement && startOther <= endPlacement)
            crossOverIndex = (uint32_t) (startOther + rand() % (endPlacement - startOther));
        else
            crossOverIndex = (uint32_t) (startPlacement + rand() % (endPlacement - startPlacement));
    }

    //uint32_t crossOverIndex = (uint32_t)round(placement.chromosome.size()/2);
    uint32_t crossOverNodeIndex = (uint32_t) round(crossOverIndex / queryOperators.size());
    for (uint32_t i = 0; i < placement.chromosome.size(); i++) {
        if (i <= crossOverIndex) {
            offspringChromosome.push_back(placement.chromosome[i]);
        } else {
            offspringChromosome.push_back(other.chromosome[i]);
        }
    }
    for (uint32_t i = 1; i < topologyNodes.size(); i++) {
        if (i < crossOverNodeIndex) {
            auto childNode = topologyNodes.at(i)->copy();
            auto pos = std::find_if(placementTopologyNodes.begin(), placementTopologyNodes.end(), [childNode](auto s) {
                return s->getId() == childNode->getId();
            });
            if (pos != placementTopologyNodes.end()) {
                auto childN = *pos;
                int parentId = childN->getParents().at(0)->as<TopologyNode>()->getId();
                auto parentNode = offspringTopology->findNodeWithId(parentId);
                offspringTopology->addNewPhysicalNodeAsChild(parentNode, childNode);
                LinkPropertyPtr linkProperty = placement.topology->findNodeWithId(parentNode->getId())
                                                   ->getLinkProperty(placement.topology->findNodeWithId(childNode->getId()));
                parentNode->addLinkProperty(childN, linkProperty);
                childN->addLinkProperty(parentNode, linkProperty);
            }

        } else {
            auto childNode = topologyNodes.at(i)->copy();
            auto childPos = std::find_if(otherTopologyNodes.begin(), otherTopologyNodes.end(), [childNode](auto s) {
                return s->getId() == childNode->getId();
            });
            if (childPos != otherTopologyNodes.end()) {
                auto childN = *childPos;
                auto oldPrent = childN->getParents().at(0)->as<TopologyNode>();
                auto parentPos = std::find_if(topologyNodes.begin(), topologyNodes.end(), [oldPrent](auto s) {
                    return s->getId() == oldPrent->getId();
                });
                if (parentPos == topologyNodes.end()) {
                    NES_DEBUG("Found no parent node!");
                }
                uint32_t parentIndex = std::distance(topologyNodes.begin(), parentPos);
                if (parentIndex >= crossOverNodeIndex) {
                    auto par = offspringTopology->findNodeWithId(oldPrent->getId());
                    offspringTopology->addNewPhysicalNodeAsChild(par, childNode);
                    LinkPropertyPtr linkProperty = childN->getLinkProperty(oldPrent);
                    par->addLinkProperty(childNode, linkProperty);
                    childNode->addLinkProperty(par, linkProperty);
                } else {
                    auto parents = topology->findNodeWithId(childN->getId())->getParents();
                    for (auto parent : parents) {
                        auto foundParent = offspringTopology->findNodeWithId(parent->as<TopologyNode>()->getId());
                        if (foundParent != nullptr) {
                            offspringTopology->addNewPhysicalNodeAsChild(foundParent, childNode);
                            LinkPropertyPtr linkProperty = topology->findNodeWithId(childNode->getId())
                                                               ->getLinkProperty(topology->findNodeWithId(foundParent->getId()));
                            foundParent->addLinkProperty(childNode, linkProperty);
                            childNode->addLinkProperty(foundParent, linkProperty);
                            break;
                        }
                    }
                }
            }
        }
    }
    offspring.chromosome = offspringChromosome;
    offspring.topology = placement.topology;
    /*
    NES_DEBUG("crossOverNodeIndex: " << crossOverNodeIndex);
    NES_DEBUG("crossOverIndex: " << crossOverIndex);
    NES_DEBUG("Placement Assignement:");
    for (uint32_t f = 0; f < queryOperators.size(); f++) {
        for (uint32_t j = 0; j < topologyNodes.size(); j++) {
            if (placement.chromosome.at(j * queryOperators.size() + f))
                NES_DEBUG("Operator " << f << " is assigned to Node " << j);
        }
    }
    NES_DEBUG("Other Assignement:");
    for (uint32_t f = 0; f < queryOperators.size(); f++) {
        for (uint32_t j = 0; j < topologyNodes.size(); j++) {
            if (other.chromosome.at(j * queryOperators.size() + f))
                NES_DEBUG("Operator " << f << " is assigned to Node " << j);
        }
    }

    NES_DEBUG("Offspring Assignment:");
    for (uint32_t f = 0; f < queryOperators.size(); f++) {
        for (uint32_t j = 0; j < topologyNodes.size(); j++) {
            if (offspring.chromosome.at(j * queryOperators.size() + f))
                NES_DEBUG("Operator " << f << " is assigned to Node " << j);
        }
    }
    */
    return offspring;
}
bool GeneticAlgorithmStrategy::offspingExists(std::vector<GeneticAlgorithmStrategy::Placement> population,
                                              GeneticAlgorithmStrategy::Placement offspring) {
    auto populationIterator = population.begin();
    while (populationIterator != population.end()) {
        if ((*populationIterator).cost == offspring.cost
            && std::equal(offspring.chromosome.begin(), offspring.chromosome.end(), (*populationIterator).chromosome.begin())) {
            return true;
        }
        ++populationIterator;
    }
    return false;
}
}// namespace NES::Optimizer