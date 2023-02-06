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

#ifndef NES_NES_CORE_INCLUDE_OPTIMIZER_QUERYPLACEMENT_ADAPTIVEACTIVESTANDBY_HPP_
#define NES_NES_CORE_INCLUDE_OPTIMIZER_QUERYPLACEMENT_ADAPTIVEACTIVESTANDBY_HPP_

#include <Operators/OperatorId.hpp>
#include <Topology/TopologyNodeId.hpp>
#include <Util/PlacementStrategy.hpp>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <unordered_map>
#include <vector>
#include <Topology/LinkProperty.hpp>

namespace NES {

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class Node;
using NodePtr = std::shared_ptr<Node>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

// unordered map that saves for every operator: the topology node where it is pinned and the score of this placement
using OperatorToTopologyMap = std::unordered_map<OperatorId, std::pair<TopologyNodeId, double>>;
// unordered map that saves for every topology node: a vector of all operators pinned on it and its topology specific score
using TopologyToOperatorMap = std::unordered_map<TopologyNodeId, std::pair<std::set<OperatorId>, double>>;

// pair< pair< vector<what to move from current node>, current node>, pair < vector<what to move from target node>, target node> >
using LocalSearchStep = std::pair<
    std::pair<std::vector<OperatorId>, TopologyNodeId>,
    std::pair<std::vector<OperatorId>, TopologyNodeId>
    >;
}// namespace NES

namespace NES::Optimizer {

class AdaptiveActiveStandby;
using AdaptiveActiveStandbyPtr = std::shared_ptr<AdaptiveActiveStandby>;

const std::string SECONDARY = "SECONDARY";
const std::string SECONDARY_OPERATOR_ID = "SECONDARY_OPERATOR_ID";

const std::string PRIMARY_OPERATOR_ID = "PRIMARY_OPERATOR_ID";

/**\brief:
 *          This class implements Adaptive Active Standby. It analyzes the topology and the placement of primary operators,
 *          deploys new topology nodes if necessary to ensure distinct path for secondary operators and determines heuristically
 *          an efficient placement for them.
 *          Current assumptions:
 *              1. Sources and sinks will not be replicated.
 *              2. Operators are not placed on sensors.
 *              3. There is only one sink.
 *              4. The pinned upstream operators are the sources.
 *              5. The pinned downstream operator is the sink.
 *              6. Shorter paths with fewer hops are preferred and chosen first.
 *              7. New topology nodes are generally deployed at higher levels in the topology.
 */
class AdaptiveActiveStandby {
  public:
    ~AdaptiveActiveStandby() = default;

    static AdaptiveActiveStandbyPtr create(TopologyPtr topology, PlacementStrategy::ValueAAS placementStrategyAAS);

    bool execute(const std::vector<OperatorNodePtr>& pinnedUpStreamOperators);

    /**
     * Get operator output value
     * @param operatorNode : the operator for which output values are needed
     * @return weight for the output
     */
    static double getOperatorOutput(const OperatorNodePtr& operatorNode);

    /**
     * Get value for operator cost
     * @param operatorNode : operator for which cost is to be computed
     * @return weight indicating operator cost
     */
    static int getOperatorCost(const OperatorNodePtr& operatorNode);

  private:
    PlacementStrategy::ValueAAS placementStrategy;
    std::map<TopologyNodeId, std::map<TopologyNodeId, double>> distances;
    // new topology node properties
    int nNewTopologyNodes = 0;
    int newTopologyNodeIdStart = 1000;      // Assumption: IDs above this reserved for new nodes
    int newOperatorNodeIdStart = 1000;
    std::string ipAddress = "localhost";
    uint32_t grpcPort = 123;
    uint32_t dataPort = 124;
    uint16_t resources = 5;
    LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));
    // constraints & weights
    std::chrono::milliseconds timeConstraint{5000};
    double overUtilizationPenaltyWeight = 0.2;
    double networkCostWeight = 0.8;
    bool excludeNodesConnectingPrimaries = true;    // false -> only exclude nodes that host the primary operators themselves

    // NOTE: replace topology->findNodeWithId(ID) with topologyMap[ID], if topologyMap contains all relevant paths instead of a
    //  single one?
    //  Explanation: Passing topologyMap from BasePlacementStrategy could be a lot more efficient because of its reduced size.
    //  At the time of this implementation, however, topology::findPathBetween only fills topologyMap with a single path between
    //  the pinned up- and downstream operators. That means that each node in topologyMap only contains the single parent along
    //  the path, and not the remaining ones. Adaptive Active Standby requires all viable paths, therefore we currently work with
    //  the entire topology, which makes finding topology nodes less performant because of a potentially large number of nodes
    //  that are not along a viable path.

    TopologyPtr topology;
    std::map<TopologyNodeId, TopologyNodePtr> sourceNodes;

    std::map<OperatorId, OperatorNodePtr> primaryOperatorMap;   // should only contain operators that are not on sources/sinks
    std::map<OperatorId, OperatorNodePtr> secondaryOperatorMap;

    // maps to keep track of the best calculated placement
    OperatorToTopologyMap bestOperatorToTopologyMap;
    TopologyToOperatorMap bestTopologyToOperatorMap;

    // maps for the currently calculated placement
    OperatorToTopologyMap candidateOperatorToTopologyMap;
    TopologyToOperatorMap candidateTopologyToOperatorMap;

    explicit AdaptiveActiveStandby(TopologyPtr topology, PlacementStrategy::ValueAAS placementStrategyAAS);

    /**
     * Deploy new topology nodes so that every operator can be replicated and have a secondary path
     * @param pinnedUpStreamOperators: vector of the pinned upstream operators
     */
    void deployNewNodes(const std::vector<OperatorNodePtr>& pinnedUpStreamOperators);

    /**
     * Check whether a primary operator's replica would have a separate path to the target from the start topology node, without
     * including the nodes where the primary and its ancestors are placed
     * @param primaryOperator: primary operator which is to be replicated
     * @param startTopologyNode: topology node where the separate path should begin
     * @param targetTopologyNode: topology node to which a separate path is searched
     */
    bool separatePathExists(const OperatorNodePtr& primaryOperator,
                            const TopologyNodePtr& startTopologyNode,
                            const TopologyNodePtr& targetTopologyNode);

    /**
     * Check whether a primary operator's replica would have a separate path to the sink from the start topology node, without
     * including the nodes where the primary and its ancestors are placed
     * @param primaryOperator: primary operator which is to be replicated
     * @param startTopologyNode: topology node where the separate path should begin
     */
    bool separatePathExists(const OperatorNodePtr& primaryOperator, const TopologyNodePtr& startTopologyNode);

    /**
     * Gets the Ids of topology nodes that host the primary operator and its ancestors up until a target topology node
     * if excludeNodesConnectingPrimaries is set to true, then it also includes the intermediate connecting nodes
     * @param primaryOperator: whose path is searched for
     * @param targetTopologyNode: where the path should end
     * @return a set of the Ids of the result topology nodes
     */
    std::set<TopologyNodeId> getNodeIdsToExcludeToTarget(const OperatorNodePtr& primaryOperator,
                                                         const TopologyNodePtr& targetTopologyNode);

    /**
     * Predicate for sorting lists of OperatorNodePtr. Sorts in ascending order, primarily based on the output, secondarily based
     * on the cost of the operators
     * @param a: first operator
     * @param b: second operator
     * @return comparison result, ascending order
     */
    static bool sortOperatorsPredicate(const NodePtr& a, const NodePtr& b);

    /**
     * Get all closest operators on all ancestor paths that are placed on a different node
     * @param operatorNode: primary operator whose ancestors are searched for
     * @param results: set of operators to which results will be added
     */
    void getAllClosestAncestorsOnDifferentNodes(const OperatorNodePtr& operatorNode, std::set<NodePtr>& results);

    /**
     * Execute Adaptive Active Standby by creating secondary operators and finding an efficient valid placement for them,
     * if such a placement exists, using a greedy algorithm. It populates the operatorMap and saves placement in
     * candidateOperatorToTopologyMap and candidateTopologyToOperatorMap
     * @param pinnedUpStreamOperators: vector of the pinned upstream operators
     * @return true if at least one replica was created and placed, otherwise false
     */
    bool executeGreedyPlacement(const std::vector<OperatorNodePtr>& pinnedUpStreamOperators);

    /**
     * Evaluate the currently stored candidate placement of secondary operators
     * @return the score of the entire placement
     */
    double evaluateEntireCandidatePlacement();

    /**
     * Execute local search by looking for incremental changes on the currently stored candidate placement.
     * For Adaptive Active Standby the result of the Greedy algorithm serves as a baseline, therefore the candidate placement
     * should store the Greedy algorithm's solution when calling this function.
     * @param timeLeft: time constraint in milliseconds
     * @return the score improvement of the resulting candidate placement compared to the one that was stored when calling this
     * function
     */
    double executeLocalSearch(std::chrono::milliseconds timeLeft);

    /**
     * Search for and evaluate topology nodes where a replica of the primary operator could be potentially placed.
     * A topology node is considered to be a valid candidate for placing a secondary operator if it allows communication with all
     * of the replicas of the primary operator's children, or the source node if primary children are placed on it. Furthermore,
     * primary and secondary operators from the same processing path must not be placed on the same nodes.
     * @param primaryOperator: the primary operator for which valid replica placements are searched. Not placed on source/sink
     * @return a map with the Ids of valid candidate topology nodes as key, with the score as value
     */
    std::map<TopologyNodeId, double> getCandidatePlacementsGreedy(const OperatorNodePtr& primaryOperator);

    /**
     * Pin a secondary operator to a topology node by adding the correct values to both candidate placement maps. Checks if the
     * operator was pinned to a different node before, unpins it from that node in that case.
     * @param secondaryOperatorId: id of secondary operator to pin
     * @param topologyNodeId: id of topology node where secondary operator is to be pinned
     */
    void pinSecondaryOperator(OperatorId secondaryOperatorId, TopologyNodeId topologyNodeId);

    /**
     * toString method for printing the current candidate operator placements.
     * For each secondary operator in the candidate placement map, it prints the primary's id and the target topology node.
     * @return formatted string of the placement map
     */
    std::string candidateOperatorPlacementsToString();

    /**
     * Get all parents of a vector of operators.
     * @param vector: operators whose parents are searched for
     * @return vector of parents, may contain duplicates
     */
    static std::vector<NodePtr> getAllParents(const std::vector<OperatorNodePtr>& vector);

    /**
     * Get all parents of a vector of operators.
     * @param vector: operators whose parents are searched for
     * @return vector of parents, may contain duplicates
     */
    static std::vector<NodePtr> getAllParents(const std::vector<NodePtr>& operators);

    /**
     * Replicates an operator by copying its properties. The replica has a different ID, and the children and parents are also
     * replicas of the corresponding operators, or are the primaries if those cannot be replicated (e.g. on source or sink nodes).
     * @param primaryOperator: operator to replicate
     * @return pointer to replica
     */
    OperatorNodePtr createReplica(const OperatorNodePtr& primaryOperator);

    /**
     * Evaluates the placement of a single secondary operator on a specific topology node by evaluating all the links to its
     * parents and children.
     * @param secondaryOperator: secondary operator that is to be evaluated
     * @param topologyNode: the topology node where placement should be evaluated
     * @return score of placing the secondary operator on the target topology node
     */
    double evaluateSinglePlacement(const OperatorNodePtr& secondaryOperator, const TopologyNodePtr& topologyNode);

    /**
     * Evaluates the placement of a single secondary operator on its current topology node by evaluating all the links to its
     * parents and children. For the final evaluation, every link between replicas is counted twice, therefore these scores are
     * halved for each replica. The connections to primary operators (e.g. sources, sinks) are only counted once.
     * Also updates the corresponding score in the candidateOperatorToTopologyMap.
     * @param secondaryOperator: secondary operator that is to be evaluated
     * @return score of the placement of the secondary operator on its current node
     */
    double evaluateSinglePlacement(const OperatorNodePtr& secondaryOperator);

    /**
     * Get all topology nodes where a vector of primary operators are placed
     * @param operatorNodes: vector of operator nodes as NodePtr whose pinned topology nodes are searched for
     * @return set of topology node ids, so that no duplicates are contained
     */
    std::set<TopologyNodeId> getAllTopologyNodesByIdWherePinned(const std::vector<NodePtr>& operatorNodes);

    /**
     * Calculate the score of the best path in a subgraph between a single start and destination node based on linkProperties.
     * @param startNode: leaf node of the subgraph, which has the destination node as its root
     * @param nodeIdToExclude: id of a specific topology node that must not be on a valid path
     * @return score of the best path
     */
    double evaluateBestPath(const TopologyNodePtr& startNode, TopologyNodeId nodeIdToExclude);

    /**
     * Find to which topology node an operator is pinned. Only works for primary operators.
     * @param operatorNode: operator whose pinned location is searched for
     * @return topology node where operator is placed
     */
    TopologyNodePtr findNodeWherePinned(const OperatorNodePtr& operatorNode);

    /**
     * Find to which topology node an operator is pinned. Only works for primary operators.
     * @param operatorNode: operator whose pinned location is searched for
     * @return topology node where operator is placed
     */
    TopologyNodePtr findNodeWherePinned(const NodePtr& operatorNode);

    /**
     * Checks if the distance between two nodes has already been calculated. Returns it if yes,
     * otherwise calls calculateDistance and stores result
     * @param start: node from which distance is to be calculated
     * @param target: node to which distance is to be calculated
     * @return sum of 1/bandwidth between each connection, 0 if start = target
     */
    double getDistance(const TopologyNodePtr& start, const TopologyNodePtr& target);

    /**
     * Calculates the distance between two nodes.
     * @param start: node from which distance is to be calculated
     * @param goal: node to which distance is to be calculated
     * @return sum of 1/bandwidth between each connection, 0 if start = target
     */
    double calculateDistance(const TopologyNodePtr& start, const TopologyNodePtr& target);

    /**
     * Calculates if the candidate node would be over-utilized if a replica of an operator would be placed there
     * @param candidateNode: node for which over-utilization is calculated
     * @param operatorNode: operator whose replica would be placed on / removed from the node
     * @param placeOrRemove: decide whether operatorNode is placed on (true) or removed from (false) the candidateNode
     * @return over-utilization penalty
     */
    double calculateOverUtilizationPenalty(const TopologyNodePtr& candidateNode, const OperatorNodePtr& operatorNode);

    /**
     * Calculates if the candidate node is over-utilized / would be over-utilized after a change of resources
     * @param candidateNode: node for which over-utilization is calculated
     * @param reserveResources: amount of resources that are to be reserved / freed up. set to 0 by default
     * @return over-utilization penalty
     */
    double calculateOverUtilizationPenalty(const TopologyNodePtr& candidateNode, int reserveResources = 0);

    /**
     * Calculates the available resources of a topology node, based on the finished primary and the candidate secondary placement
     * @param topologyNode: whose resources are calculated
     * @return available resources (negative if over-utilized)
     */
    int calculateAvailableResources(const TopologyNodePtr& topologyNode);

    /**
     * Assigns operators to topology nodes based on the best candidate, reduces resources accordingly
     * @return true if successful
     */
    bool pinOperators();


    /**
     * Get the primary operator of a secondary
     * @param secondaryOperator: replica whose primary is searched for
     * @return OperatorNodePtr to primary
     */
    OperatorNodePtr getPrimaryOperatorOfSecondary(const OperatorNodePtr& secondaryOperator);

    /**
     * Calculates and evaluates all possible steps for local search for a replica
     * @param secondaryToMove: replica that is to be moved
     * @return best step as a pair of LocalSearchStep and its score change
     */
    std::pair<LocalSearchStep, double> getBestStepLocalSearch(const OperatorNodePtr& secondaryToMove);

    /**
     * Evaluates a simple local search step, which consists of either moving a single operator from its current node to another
     * (targetSecondary = nullptr), or of swapping two single operators from their current nodes (targetSecondary required).
     * The validity has to be checked before calling the function.
     * @param currentSecondary: secondary to move/swap
     * @param currentNode: current node of currentSecondary
     * @param targetNode: node to which currentSecondary is to be moved
     * @param targetSecondary: optional, secondary operator on targetNode with which currentSecondary is to be swapped
     * @return Pair of corresponding LocalSearchStep and its calculated change of the current score.
     */
    std::pair<LocalSearchStep, double> evaluateSimpleStepLocalSearch(const OperatorNodePtr& currentSecondary,
                                                                     const TopologyNodePtr& currentNode,
                                                                     const TopologyNodePtr& targetNode,
                                                                     const OperatorNodePtr& targetSecondary = nullptr);
    /**
     * Create a string from a LocalSearchStep.
     * @param step: from which string is to be created
     * @return string
     */
    static std::string localSearchStepToString(const LocalSearchStep& step);
};
}// namespace NES::Optimizer

#endif//NES_NES_CORE_INCLUDE_OPTIMIZER_QUERYPLACEMENT_ADAPTIVEACTIVESTANDBY_HPP_
