#ifndef BASE_nodeERATOR_NODE_HPP
#define BASE_nodeERATOR_NODE_HPP

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
    void addSuccessor(const NodePtr& newNode);

    /**
     * @brief remove a node from current successors.
     * @param node
     */
    bool removeSuccessor(const NodePtr& node);

    /**
     * @brief add a predecessor to vector of predecessors
     *        no duplicated nodeerator inside predecessors.
     *        one cannot add current nodeerator into its predecessors.
     * @param newNode
     */
    void addPredecessor(const NodePtr& newNode);

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
     * @brief remove the given nodeerator together with its successors
     * @param node the given nodeerator to remove
     * @return bool true if
     */
    bool remove(const NodePtr& node);

    /**
     * @brief remove the given nodeerator and level up its successors
     * @param node

     * @return bool true if
     */
    bool removeAndLevelUpSuccessors(const NodePtr& node);

    /**
     * @brief clear all predecessors and successors
     */
    void clear();

    virtual const std::string toString() const = 0;

    /**
     * @brief get the id of an nodeerator
     * @return string
     */
    const std::string getnodeeratorId() const;
    void setnodeeratorId(const std::string& id);

    const std::vector<NodePtr>& getSuccessors() const;
    const std::vector<NodePtr>& getPredecessors() const;

    bool equalWithAllSuccessors(const NodePtr& rhs);
    bool equalWithAllPredecessors(const NodePtr& rhs);

    /**
     * @brief check two node are equal or not. Noting they could be
     *        two different objects with the same value.
     * @param rhs the node to compare
     * @return bool true if they are the same otherwise false
     * @TODO should be a pure virtual function
     */
    virtual bool equal(const NodePtr& rhs) const {
        return false;
    };

    /**
     * @brief check two nodeerators whether are exactly the same object or not
     * @param rhs the nodeerator to check
     * @return bool true if they are the same object otherwise false
     */
    virtual bool isIdentical(const NodePtr& rhs) const {
        return rhs.get() == this;
    };

    /**
     * @see isIdentical() function
     */
    virtual bool isNotIdentical(const NodePtr& rhs) const {
        return rhs.get() != this;
    };

    /**
     * @brief split graph into multiple sub-graphs. The graph starts at current nodeerator
     *        if the given nodeerator is not in the graph, throw exception
     * @params node the given nodeerator to split at.
     * @return vector of multiple sub-graphs.
     */
    std::vector<NodePtr> split(const NodePtr& node);

    /**
     * @brief validation of this nodeerator
     * @return true if there is no ring/lonode inside this nodeerator's successors, otherwise false
     */
    bool isValid();

    template<class NodeType>
    const bool instanceOf() {
        if (dynamic_cast<NodeType*>(this)) {
            return true;
        };
        return false;
    };

    template<class NodeType>
    std::shared_ptr<NodeType> as() {
        if (instanceOf<NodeType>()) {
            return std::dynamic_pointer_cast<NodeType>(this->shared_from_this());
        } else {
            NES_FATAL_ERROR("We performed an invalid cast");
            throw std::bad_cast();
        }
    }

    template<class NodeType>
    std::vector<std::shared_ptr<NodeType>> getNodesByType() {
        std::vector<std::shared_ptr<NodeType>> vector;
        getNodesByTypeHelper<NodeType>(vector);
        return vector;
    }

    template<class NodeType>
    void getNodesByTypeHelper(std::vector<std::shared_ptr<NodeType>>& foundNodes) {
        if (this->instanceOf<NodeType>()) {
            foundNodes.push_back(this->as<NodeType>());
        }
        for (auto& successor:this->successors) {
            successor->getNodesByTypeHelper(foundNodes);
        }
    };

    bool isCyclic();

    /**
     * @brief return all successors of current nodeerator
     *        Always excluding current nodeerator, no matter a cycle exists
     * @params allChildren a vector to store all successors of current nodeerator
     */
    std::vector<NodePtr> getAndFlattenAllSuccessors();

    void prettyPrint(std::ostream& out = std::cout);
  protected:
    /**
     * @brief the nodeerator id would be set as uuid to ensure uniqueness
     *        the main reason is that we'd like make sure all nodeerators in
     *        the given graph (tree) should be unique.
     */
    const std::string nodeId;
    /**
     * @brief the predecessors of this nodeerator. There is no equal nodeerators
     *        in this vector
     */
    std::vector<NodePtr> predecessors{};
    /**
     * @brief the successors of this nodeerator. There is no equal nodeerators
     *        in this vector
     */
    std::vector<NodePtr> successors{};
  private:
    /**
     * @brief check if an nodeerator is in given vector or not
     * @param nodeeratorNodes
     * @param node
     * @return return true if the given nodeerator is found, otherwise false
     */
    NodePtr find(const std::vector<NodePtr>& nodeeratorNodes, const NodePtr& node);
    /**
     * @brief check if an nodeerator is in given graph
     * @param root
     * @param node
     * @return return true if the given nodeerator is found in the graph of root, otherwise false
     */
    NodePtr findRecursively(const NodePtr& root, const NodePtr& node);


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
     *
     */

    // bool predicateFunc(std::vector<NodePtr>&,
    //                    NodePtr& node,
    //                    Node& excludednode,
    //                    nodeeratorType& type));
    // bool predicateFunc(std::vector<NodePtr>& allChildren, NodePtr& node, Node& excludednode, nodeeratorType& type) {
    //     return (!find(allChildren, node) && (node.get() != &excludednode));
    // }

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
  public:
    bool visited;
    bool recStack;
};
}      // namespace NES

#endif  // BASE_nodeERATOR_NODE_HPP
