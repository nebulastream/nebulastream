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

#ifndef NES_INCLUDE_TOPOLOGY_TOPOLOGY_HPP_
#define NES_INCLUDE_TOPOLOGY_TOPOLOGY_HPP_

#include <any>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>
#ifdef S2DEF
#include <s2/s2point_index.h>
#endif

namespace NES {

//if no other values is supplied to the getClosestNodePosition() function, it will use this search radius to try to find a node
const int STANDARD_SEARCH_RADIUS = 50;
class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

/**
 * @brief This class represents the overall physical infrastructure with different nodes
 */
class Topology {

  public:
    /**
     * @brief Factory to create instance of topology
     * @return shared pointer to the topology
     */
    static TopologyPtr create();

    /**
     * @brief Get the root node of the topology
     * @return root of the topology
     */
    TopologyNodePtr getRoot();

    /**
     * @brief This method will add the new node as child to the provided parent node
     * @param parent : the pointer to the parent physical node
     * @param newNode : the new physical node.
     * @return true if successful
     */
    bool addNewTopologyNodeAsChild(const TopologyNodePtr& parent, const TopologyNodePtr& newNode);

    /**
     * @brief This method will remove a given physical node
     * @param nodeToRemove : the node to be removed
     * @return true if successful
     */
    bool removePhysicalNode(const TopologyNodePtr& nodeToRemove);

    /**
     * @brief This method sets the location of a new node (making it a field node) or updates the position of an existing field node
     * @param node: a pointer to the topology node
     * @param coordinates: the (new) location of the field node
     * @param init: defines if the method is called as part of node creation or later on. trying to set a location for a node
     * without existing coordinates will result in failure if the init flag is not set, thus preventing the updating of
     * non field nodes to become field nodes
     * @return true if successful
     */
    bool setPhysicalNodePosition(const TopologyNodePtr& node, std::tuple<double, double> coordinates, bool init = false);

    //TODO: make these functions also return optional?
    /**
     * @brief returns the closest field node to a certain geographical location
     * @param coordTuple coordinates of a location on the map
     * @param radius the maximum distance which the returned node can habe from the specified location
     * @return TopologyNodePtr to the closest field node
     */
    TopologyNodePtr getClosestNodeTo(std::tuple<double, double> coordTuple, int radius = STANDARD_SEARCH_RADIUS);

    /**
     * @brief returns the closest field node to a cartein node (which does not equal the node passed as an argument)
     * @param nodePtr: pointer to a field node
     * @param radius the maximum distance which the returned node can habe from the specified node
     * @return TopologyNodePtr to the closest field node unequal to nodePtr
     */
    TopologyNodePtr getClosestNodeTo(const TopologyNodePtr& nodePtr, int radius = STANDARD_SEARCH_RADIUS);

    /**
     * @brief get a list of all the nodes within a certain radius around a location
     * @param center: a point on the map around which we look for nodes
     * @param radius: the maximum distance of the returned nodes from center
     * @return a vector of pairs containing node pointers and the corresponding locations
     */
    std::vector<std::pair<TopologyNodePtr, std::tuple<double, double>>> getNodesInRange(std::tuple<double, double> center, double radius);
    /**
     *
     * @return the amount of field nodes in the system
     */
    size_t getSizeOfPointIndex();

    /**
     * @brief This method will find a given physical node by its id
     * @param nodeId : the id of the node
     * @return physical node if found else nullptr
     */
    TopologyNodePtr findNodeWithId(uint64_t nodeId);

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
     * @param sourceNodes: the start Serialization
     * @param destinationNodes: the destination Serialization
     * @return a vector of start nodes of the sub-graph if all start nodes can connect to all destination nodes else an empty vector
     */
    std::vector<TopologyNodePtr> findPathBetween(const std::vector<TopologyNodePtr>& sourceNodes,
                                                 const std::vector<TopologyNodePtr>& destinationNodes);

    /**
     * @brief a Physical Node with the ip address and grpc port exists
     * @param ipAddress: ipaddress of the node
     * @param grpcPort: grpc port of the node
     * @return true if node exists else false
     */
    bool nodeExistsWithIpAndPort(const std::string& ipAddress, uint32_t grpcPort);

    /**
     * @brief Print the current topology information
     */
    void print();

    /**
     * @brief Return graph as string
     * @return string object representing topology information
     */
    std::string toString();

    /**
     * @brief Add a Physical node as the root node of the topology
     * @param physicalNode: Physical node to be set as root node
     */
    void setAsRoot(const TopologyNodePtr& physicalNode);

    /**
     * @brief Remove links between
     * @param parentNode
     * @param childNode
     * @return
     */
    bool removeNodeAsChild(const TopologyNodePtr& parentNode, const TopologyNodePtr& childNode);

    /**
     * @brief Increase the amount of resources on the node with the id
     * @param nodeId : the node id
     * @param amountToIncrease : resources to free
     * @return true if successful
     */
    bool increaseResources(uint64_t nodeId, uint16_t amountToIncrease);

    /**
     * @brief Reduce the amount of resources on the node with given id
     * @param nodeId : the node id
     * @param amountToReduce : amount of resources to reduce
     * @return true if successful
     */
    bool reduceResources(uint64_t nodeId, uint16_t amountToReduce);

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
    * @brief Looks for the TopologyNode with id in the vector of sourceNodes and their parents
    * @param sourceNodes : the source topology nodes
    * @param id : the id of the topology node that is being searched for
    * @return nullptr if node is not found, otherwise TopologyNodePtr
    */
    static TopologyNodePtr findTopologyNodeInSubgraphById(uint64_t id, const std::vector<TopologyNodePtr>& sourceNodes);


  private:
    static constexpr int BASE_MULTIPLIER = 10000;

    explicit Topology();

    /**
     * @brief Find if searched node is in the parent list of the test node or its parents parent list
     * @param testNode: the test node
     * @param searchedNodes: the searched node
     * @return the node where the searched node is found
     */
    TopologyNodePtr
    find(TopologyNodePtr testNode, std::vector<TopologyNodePtr> searchedNodes, std::map<uint64_t, TopologyNodePtr>& uniqueNodes);

    //TODO: At present we assume that we have only one root node
    TopologyNodePtr rootNode;
    std::mutex topologyLock;
    std::map<uint64_t, TopologyNodePtr> indexOnNodeIds;

#ifdef S2DEF
    // a spatial index that stores pointers to all the field nodes
    S2PointIndex<TopologyNodePtr> nodePointIndex;
#endif
};
}// namespace NES
#endif  // NES_INCLUDE_TOPOLOGY_TOPOLOGY_HPP_
