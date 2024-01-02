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
 * @brief This is a utility class that operates on the topology nodes to find different types of topology nodes between
 * given pair of topology nodes.
 */
class PathFinder {

  public:
    PathFinder(WorkerId rootTopologyNodeId);

    virtual ~PathFinder() = default;

    /**
     * @brief Find the immediate common ancestor for the set of Topology nodes
     * @param topologyNodes: the set of topology nodes
     * @return the immediate common ancestor (when many then the first identified ancestor is given) else nullptr.
     */
    TopologyNodePtr findCommonAncestor(std::vector<TopologyNodePtr> topologyNodes);

    /**
     * @brief Find the immediate common child for the set of Topology nodes
     * @param topologyNodes: the set of topology nodes
     * @return the immediate common child (when many then the first identified ancestor is given) else nullptr.
     */
    TopologyNodePtr findCommonChild(std::vector<TopologyNodePtr> topologyNodes);

    /**
     * @brief Find a node location that can be reachable from both the
     * @param childNodes: list of child nodes to be reachable
     * @param parenNodes: list of parent nodes to be reachable
     * @return common node (when many then the first identified ancestor is given) else nullptr.
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

  private:
    WorkerId rootTopologyNodeId;
};
}// namespace NES

#endif//NES_PATHFINDER_HPP
