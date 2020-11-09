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

#ifndef NES_TOPOLOGY_HPP
#define NES_TOPOLOGY_HPP

#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace NES {

class TopologyNode;
typedef std::shared_ptr<TopologyNode> TopologyNodePtr;

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

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
    bool addNewPhysicalNodeAsChild(TopologyNodePtr parent, TopologyNodePtr newNode);

    /**
     * @brief This method will remove a given physical node
     * @param nodeToRemove : the node to be removed
     * @return true if successful
     */
    bool removePhysicalNode(TopologyNodePtr nodeToRemove);

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
    std::optional<TopologyNodePtr> findAllPathBetween(TopologyNodePtr startNode, TopologyNodePtr destinationNode);

    /**
     * @brief Find a sub-graph such that each start node in the given set of start nodes can connect to each destination node in the given set of destination nodes.
     * @param sourceNodes: the start Nodes
     * @param destinationNodes: the destination Nodes
     * @return a vector of start nodes of the sub-graph if all start nodes can connect to all destination nodes else an empty vector
     */
    std::vector<TopologyNodePtr> findPathBetween(std::vector<TopologyNodePtr> sourceNodes, std::vector<TopologyNodePtr> destinationNodes);

    //FIXME: as part of the issue #955
    //    std::vector<LinkProperties> getLinkPropertiesBetween(PhysicalNodePtr startNode, PhysicalNodePtr destinationNde);
    //    std::vector<LinkProperties> getInputLinksForNode(PhysicalNodePtr node);
    //    std::vector<LinkProperties> getOutputLinksForNode(PhysicalNodePtr node);

    /**
     * @brief a Physical Node with the ip address and grpc port exists
     * @param ipAddress: ipaddress of the node
     * @param grpcPort: grpc port of the node
     * @return true if node exists else false
     */
    bool nodeExistsWithIpAndPort(std::string ipAddress, uint32_t grpcPort);

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
    void setAsRoot(TopologyNodePtr physicalNode);

    /**
     * @brief Remove links between
     * @param parentNode
     * @param childNode
     * @return
     */
    bool removeNodeAsChild(TopologyNodePtr parentNode, TopologyNodePtr childNode);

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
    std::vector<TopologyNodePtr> mergeSubGraphs(std::vector<TopologyNodePtr> startNodes);

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
    TopologyNodePtr findCommonChild(std::vector<TopologyNodePtr> topologyNodes);

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
    std::vector<TopologyNodePtr> findTopologyNodesBetween(TopologyNodePtr sourceNode, TopologyNodePtr destinationNode);

    ~Topology();

  private:
    static constexpr int BASE_MULTIPLIER = 10000;

    explicit Topology();

    /**
     * @brief Find if searched node is in the parent list of the test node or its parents parent list
     * @param testNode: the test node
     * @param searchedNodes: the searched node
     * @return the node where the searched node is found
     */
    TopologyNodePtr find(TopologyNodePtr testNode, std::vector<TopologyNodePtr> searchedNodes, std::map<uint64_t, TopologyNodePtr>& uniqueNodes);

    //TODO: At present we assume that we have only one root node
    TopologyNodePtr rootNode;
    std::mutex topologyLock;
    std::map<uint64_t, TopologyNodePtr> indexOnNodeIds;
};
}// namespace NES
#endif//NES_TOPOLOGY_HPP
