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

  private:
    explicit Topology(PhysicalNodePtr rootNode);

    //TODO: At present we assume that we have only one root node
    PhysicalNodePtr rootNode;
    std::mutex mutex;
};
}// namespace NES
#endif//NES_TOPOLOGY_HPP
