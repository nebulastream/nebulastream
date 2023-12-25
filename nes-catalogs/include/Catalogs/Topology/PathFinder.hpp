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

#ifndef NES_PATHFINDER_HPP
#define NES_PATHFINDER_HPP

#include <Identifiers.hpp>
#include <map>
#include <memory>
#include <optional>
#include <vector>

namespace NES {

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

/**
 * @brief This is a utility class that help path between topology nodes
 */
class PathFinder {

  public:
    PathFinder(WorkerId rootTopologyNodeId);

    /**
     * @brief Merge the sub graphs starting from the nodes into a single sub-graph
     * @param startNodes : start nodes of the sub-graphs to be merged
     * @return start nodes of the merged sub-graph
     */
    static std::vector<TopologyNodePtr> mergeSubGraphs(const std::vector<TopologyNodePtr>& startNodes);

    /**
     * @brief Find the immediate common ancestor for the set of Topology nodes
     * @param topologyNodes: the set of topology nodes
     * @return the immediate common ancestor.
     */
    TopologyNodePtr findCommonAncestor(std::vector<TopologyNodePtr> topologyNodes);

    /**
     * @brief Find the immediate common child for the set of Topology nodes
     * @param topologyNodes: the set of topology nodes
     * @return the immediate common child.
     */
    static TopologyNodePtr findCommonChild(std::vector<TopologyNodePtr> topologyNodes);

    /**
     * @brief Find a node location that can be reachable from both the
     * @param childNodes: list of child nodes to be reachable
     * @param parenNodes: list of parent nodes to be reachable
     * @return common node else nullptr
     */
    TopologyNodePtr findCommonNodeBetween(std::vector<TopologyNodePtr> childNodes, std::vector<TopologyNodePtr> parenNodes);

    /**
     * @brief Find the set of nodes (inclusive of) between a source and destination topology node
     * @param sourceNode : the source topology node
     * @param destinationNode : the destination topology node
     * @return returns a vector of nodes (inclusive of) between a source and destination topology node if no path exists then an empty vector
     */
    std::vector<TopologyNodePtr> findNodesBetween(const TopologyNodePtr& sourceNode, const TopologyNodePtr& destinationNode);

    /**
     * @brief Find the set of shared nodes (inclusive of) between a set of source and destination topology nodes
     * @param sourceNodes : the source topology nodes
     * @param destinationNodes : the destination topology nodes
     * @return returns a vector of nodes (inclusive of) between a source and destination topology node if no path exists then an empty vector
     */
    std::vector<TopologyNodePtr> findNodesBetween(std::vector<TopologyNodePtr> sourceNodes,
                                                  std::vector<TopologyNodePtr> destinationNodes);

    /**
     * @brief This method will return a subgraph containing all the paths between start and destination node.
     * Note: this method will only look for the destination node only in the parent nodes of the start node.
     * @param startNode: the physical start node
     * @param destinationNode: the destination start node
     * @return physical location of the leaf node in the graph.
     */
    std::optional<TopologyNodePtr> findAllPathBetween(const TopologyNodePtr& startNode, const TopologyNodePtr& destinationNode);

    /**
     * @brief Find a sub-graph such that each start node in the given set of start nodes can connect to each destination node in the given set of destination nodes.
     * @param sourceNodes: the topology nodes where to start the path identification
     * @param destinationNodes: the topology nodes where to end the path identification
     * @return a vector of start/upstream topology nodes of the sub-graph if all start nodes can connect to all destination nodes else an empty vector
     */
    std::vector<TopologyNodePtr> findPathBetween(const std::vector<TopologyNodePtr>& sourceNodes,
                                                 const std::vector<TopologyNodePtr>& destinationNodes);

  private:
    /**
     * @brief Find if searched node is in the parent list of the test node or its parents parent list
     * @param testNode: the test node
     * @param searchedNodes: the searched node
     * @return the node where the searched node is found
     */
    TopologyNodePtr
    find(TopologyNodePtr testNode, std::vector<TopologyNodePtr> searchedNodes, std::map<WorkerId, TopologyNodePtr>& uniqueNodes);
    WorkerId rootTopologyNodeId;
    static constexpr int BASE_MULTIPLIER = 10000;
};
}// namespace NES

#endif//NES_PATHFINDER_HPP
