#ifndef NODES_NODE_HPP
#define NODES_NODE_HPP

#include <iostream>
#include <vector>
#include <memory>
#include <Util/Logger.hpp>

namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

class Node : public std::enable_shared_from_this<Node> {
  public:
    Node();
    ~Node();

    /**
     * @brief adds a newNode as a successor to the current newNode.
     * Duplicates inside the successor are ignored.
     * A newNode cannot be in its own successor.
     * @param newNode
     */
    bool addSuccessor(const NodePtr& newNode);

    /**
     * @brief remove a node from current successors.
     * @param node
     */
    bool removeSuccessor(const NodePtr& node);

    /**
     * @brief add a predecessor to vector of predecessors
     *        no duplicated node inside predecessors.
     *        one cannot add current node into its predecessors.
     * @param newNode
     */
    bool addPredecessor(const NodePtr& newNode);

    /**
     * @brief remove a predecessor from vector of predecessors
     * @param node
     */
    bool removePredecessor(const NodePtr& node);

    /**
     * @brief replace an old node with new now
     * 1) old node is the successor of current node, remove old node
     *    from current nodes's successors and add new node to current noed's
     *    successors. If old node has successors, merge the successors of old nodes
     *    and new nodes's successors. If there's duplicated successors among old nodes and
     *    new nodes's successors, the successors in new noeds will overwrite that
     *    inside old noeds's.
     * 2)
     * @param newNode
     * @param oldNode
     */
    bool replace(NodePtr newNode, NodePtr oldNode);

    /**
     * @brief swap given old node by new node
     * @param newNode the node to mount at oldNode predecessors instead of oldNode
     * @param oldNode the node to remove from graph
     * @return true if swapping successfully otherwise false
     */
    bool swap(const NodePtr& newNode, const NodePtr& oldNode);

    /**
     * @brief remove the given node together with its successors
     * @param node the given node to remove
     * @return bool true if successful
     */
    bool remove(const NodePtr& node);

    /**
     * @brief remove the given node and add its successors as to the successors of the current node
     * @param node
     * @return bool true if successful
     */
    bool removeAndLevelUpSuccessors(const NodePtr& node);

    /**
     * @brief clear all predecessors and successors
     */
    void clear();

    /**
     * @brief checks if the current node and its successors are equal a other node and its successors
     * @param node the node to compare
     * @return bool
     */
    bool equalWithAllSuccessors(const NodePtr& node);

    /**
     * @brief checks if the current node and its predecessors are equal a other node and its predecessors
     * @param node the node to compare
     * @return bool
     */
    bool equalWithAllPredecessors(const NodePtr& node);

    /**
     * @brief check two node are equal.
     * @param rhs the node to compare
     * @return bool true if they are the same otherwise false
     */
    virtual bool equal(const NodePtr& rhs) const {
        return this->isIdentical(rhs);
    };

    /**
     * @brief check two nodes whether are exactly the same object or not
     * @param rhs the node to check
     * @return bool true if they are the same object otherwise false
     */
    virtual bool isIdentical(const NodePtr& rhs) const {
        return rhs.get() == this;
    };

    /**
     * @brief split graph into multiple sub-graphs. The graph starts at current splitNode.
     * If the given splitNode is not in the graph, throw exception
     * @params splitNode the given splitNode to split at.
     * @return vector of multiple sub-graphs.
     */
    std::vector<NodePtr> split(const NodePtr& splitNode);

    /**
     * @brief validation of this node
     * @return true if there is no ring/node inside this node's successors, otherwise false
     */
    bool isValid();

    /**
     * @brief Checks if the current node is of type NodeType
     * @tparam NodeType
     * @return bool true if node is of NodeType
     */
    template<class NodeType>
    const bool instanceOf() {
        if (dynamic_cast<NodeType*>(this)) {
            return true;
        };
        return false;
    };

    /**
    * @brief Dynamically casts the node to a NodeType
    * @tparam NodeType
    * @return returns a shared pointer of the NodeType
    */
    template<class NodeType>
    std::shared_ptr<NodeType> as() {
        if (instanceOf<NodeType>()) {
            return std::dynamic_pointer_cast<NodeType>(this->shared_from_this());
        } else {
            NES_FATAL_ERROR("We performed an invalid cast");
            throw std::bad_cast();
        }
    }

    /**
     * @brief Collects all nodes that are of a specific node type, e.g. all FilterOperatorNodes.
     * @tparam NodeType
     * @return vector of nodes
     */
    template<class NodeType>
    std::vector<std::shared_ptr<NodeType>> getNodesByType() {
        std::vector<std::shared_ptr<NodeType>> vector;
        getNodesByTypeHelper<NodeType>(vector);
        return vector;
    }

    /**
     * @brief checks if the node and its successors contain cycles.
     * @return true if cyclic
     */
    bool isCyclic();

    /**
     * @brief return all successors of current node
     *        Always excluding current node, no matter a cycle exists
     * @params allChildren a vector to store all successors of current node
     */
    std::vector<NodePtr> getAndFlattenAllSuccessors();

    /**
     * @brief get direct successors.
     * @return vector of successors.
     */
    const std::vector<NodePtr>& getSuccessors() const;

    /**
     * @brief get direct predecessors.
     * @return vector of predecessors.
     */
    const std::vector<NodePtr>& getPredecessors() const;

    virtual const std::string toString() const = 0;

    void prettyPrint(std::ostream& out = std::cout);

  protected:
    /**
     * @brief the predecessors of this node. There is no equal nodes
     *        in this vector
     */
    std::vector<NodePtr> predecessors{};
    /**
     * @brief the successors of this node. There is no equal nodes
     *        in this vector
     */
    std::vector<NodePtr> successors{};
  private:
    /**
     * @brief check if an node is in given vector or not
     * @param nodes
     * @param nodeToFind
     * @return return true if the given node is found, otherwise false
     */
    bool contains(const std::vector<NodePtr>& nodes, const NodePtr& nodeToFind);

    /**
     * @brief check if an node is in given vector and returns it
     * @param nodes
     * @param nodeToFind
     * @return return node if the given node is found, otherwise nullpointer
     */
    NodePtr find(const std::vector<NodePtr>& nodes, const NodePtr& nodeToFind);
    /**
     * @brief check if an nodeToFind is in given graph
     * @param root
     * @param nodeToFind
     * @return return true if the given nodeToFind is found in the graph of root, otherwise false
     */
    NodePtr findRecursively(const NodePtr& root, const NodePtr& nodeToFind);


    /********************************************************************************
     *                   Helper functions                                           *
     ********************************************************************************/
    /**
     * @brief helper function of equalWithAllPredecessors() function
     */
    bool equalWithAllPredecessorsHelper(const NodePtr& node1, const NodePtr& node2);
    /**
     * @brief helper function of equalWithAllSuccessors() function
     */
    bool equalWithAllSuccessorsHelper(const NodePtr& node1, const NodePtr& node2);
    /**
     * @brief helper function of prettyPrint() function
     */
    void printHelper(const NodePtr& node, size_t depth, size_t indent, std::ostream& out) const;
    /**
     * @brief helper function of getAndFlattenAllSuccessors() function
     */
    void getAndFlattenAllSuccessorsHelper(const NodePtr& node,
                                          std::vector<NodePtr>& allChildren,
                                          const NodePtr& excludednode);

    /**
     * @brief helper function of getNodeType() function
     */
    template<class NodeType>
    void getNodesByTypeHelper(std::vector<std::shared_ptr<NodeType>>& foundNodes) {
        if (this->instanceOf<NodeType>()) {
            foundNodes.push_back(this->as<NodeType>());
        }
        for (auto& successor:this->successors) {
            successor->getNodesByTypeHelper(foundNodes);
        }
    };

    /**
     * @brief helper function of cycle detector
     */
    bool isCyclicHelper(Node& node);

    /********************************************************************************
     *                   Helper parameters                                           *
     ********************************************************************************/
    /**
     * Helper parameters for cycle detection
     */
    bool visited;
    bool recStack;
};
}// namespace NES

#endif  // NODES_NODE_HPP
