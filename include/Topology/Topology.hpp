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
    std::optional<TopologyNodePtr> findAllPathBetween(PhysicalNodePtr startNode, PhysicalNodePtr destinationNode);

    /**
     * @brief Find a sub-graph such that each start node in the given set of start nodes can connect to each destination node in the given set of destination nodes.
     * @param startNodes: the start Nodes
     * @param destinationNodes: the destination Nodes
     * @return a vector of start nodes of the sub-graph if all start nodes can connect to all destination nodes else an empty vector
     */
    std::vector<TopologyNodePtr> findPathBetween(std::vector<PhysicalNodePtr> startNodes, std::vector<PhysicalNodePtr> destinationNodes);

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

    ~Topology();

  private:
    explicit Topology();

    bool find(TopologyNodePtr physicalNode, TopologyNodePtr destinationNode, std::vector<TopologyNodePtr>& nodesInPath);
    /**
     * @brief Find if searched node is in the parent list of the test node or its parents parent list
     * @param testNode: the test node
     * @param searchedNodes: the searched node
     * @return the node where the searched node is found
     */
    TopologyNodePtr find(TopologyNodePtr testNode, std::vector<TopologyNodePtr> searchedNodes, std::map<uint64_t, TopologyNodePtr>& uniqueNodes);

    //TODO: At present we assume that we have only one root node
    TopologyNodePtr rootNode;
    std::mutex mutex;
    std::map<uint64_t, TopologyNodePtr> indexOnNodeIds;
};
}// namespace NES
#endif//NES_TOPOLOGY_HPP
