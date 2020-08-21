#ifndef NES_TOPOLOGY_HPP
#define NES_TOPOLOGY_HPP

#include <memory>
#include <vector>
#include <mutex>

namespace NES {

class PhysicalNode;
typedef std::shared_ptr<PhysicalNode> PhysicalNodePtr;

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

/**
 * @brief This class represents the overall physical infrastructure with different nodes
 */
class Topology {

    /**
     * @brief Factory to create instance of topology
     * @param rootNode: the root node
     * @return shared pointer to the topology
     */
    static TopologyPtr create(PhysicalNodePtr rootNode);

    /**
     * @brief This method will add the new node as child to the provided parent node
     * @param parent : the pointer to the parent physical node
     * @param newNode : the new physical node.
     * @return true if successful
     */
    bool addNewPhysicalNodeAsChild(PhysicalNodePtr parent, PhysicalNodePtr newNode);

    /**
     * @brief This method will add the new node as parent to the provided child node
     * @param child : the pointer to the child physical node
     * @param newNode : the pointer to the new physical node
     * @return true if successful
     */
    bool addNewPhysicalNodeAsParent(PhysicalNodePtr child, PhysicalNodePtr newNode);

    /**
     * @brief This method will remove a given physical node
     * @param nodeToRemove : the node to be removed
     * @return true if successful
     */
    bool removePhysicalNode(PhysicalNodePtr nodeToRemove);

    /**
     * @brief This method will return a subgraph containing only the path between start and destination node.
     * Note: this method will only look for the destination node only in the parent nodes of the start node.
     * @param startNode: the physical start node
     * @param destinationNode: the destination start node
     * @return physical location of the leaf node in the graph.
     */
    PhysicalNodePtr findPathBetween(PhysicalNodePtr startNode, PhysicalNodePtr destinationNode);

  private:
    explicit Topology(PhysicalNodePtr rootNode);

    //TODO: At present we assume that we have only one root node
    PhysicalNodePtr rootNode;
    std::mutex mutex;
    bool find(PhysicalNodePtr physicalNode, PhysicalNodePtr destinationNode, std::vector<PhysicalNodePtr>& nodesInPath);
};
}// namespace NES
#endif//NES_TOPOLOGY_HPP
