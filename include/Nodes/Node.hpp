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

#ifndef NODES_NODE_HPP
#define NODES_NODE_HPP

#include <Util/Logger.hpp>
#include <iostream>
#include <memory>
#include <vector>

namespace NES {

class Node;
using NodePtr = std::shared_ptr<Node>;

class Node : public std::enable_shared_from_this<Node> {
  public:
    Node();
    virtual ~Node() = default;

    /**
     * @brief adds a newNode as a child to the current newNode.
     * @note Duplicates inside the children are not ignored.
     * @note A newNode cannot be in its own child.
     * @param newNode
     */
    bool addChildWithEqual(NodePtr const& newNode);

    /**
     * @brief adds a newNode as a child to the current newNode.
     * @note Duplicates inside the children are ignored.
     * @note  A newNode cannot be in its own child.
     * @param newNode
     */
    virtual bool addChild(NodePtr newNode);

    /**
     * @brief remove a node from current children.
     * @param node
     */
    bool removeChild(NodePtr const& node);

    /**
     * @brief add a parent to vector of parents
     *        no duplicated node inside parents.
     *        one cannot add current node into its parents.
     * @param newNode
     */
    virtual bool addParent(NodePtr newNode);

    /**
     * @brief remove a parent from vector of parents
     * @param node
     */
    bool removeParent(NodePtr const& node);

    /**
     * @brief Remove all parents
     */
    void removeAllParent();

    /**
     * @brief Remove all children
     */
    void removeChildren();

    /**
     * @brief replace an old node with new now
     * 1) old node is the child of current node, remove old node
     *    from current nodes's children and add new node to current noed's
     *    children. If old node has children, unionWith the children of old nodes
     *    and new nodes's children. If there's duplicated children among old nodes and
     *    new nodes's children, the children in new noeds will overwrite that
     *    inside old noeds's.
     * 2)
     * @param newNode
     * @param oldNode
     */
    bool replace(const NodePtr& newNode, const NodePtr& oldNode);

    /**
     * @brief replace current node with new node
     * @param newNode
     * @return
     */
    bool replace(NodePtr newNode);
    /**
     * @brief swap given old node by new node
     * @param newNode the node to mount at oldNode parents instead of oldNode
     * @param oldNode the node to remove from graph
     * @return true if swapping successfully otherwise false
     */
    bool swap(NodePtr const& newNode, NodePtr const& oldNode);

    /**
     * @brief remove the given node together with its children
     * @param node the given node to remove
     * @return bool true if successful
     */
    bool remove(NodePtr const& node);

    /**
     * @brief remove the given node and add its children to the children of the current node
     * @param node
     * @return bool true if successful
     */
    bool removeAndLevelUpChildren(NodePtr const& node);

    /**
     * @brief Remove this node as child to its parents and as parent to its children. Once done, the method joins the
     * parent and children together.
     * @return bool true if successful
     */
    bool removeAndJoinParentAndChildren();

    /**
     * @brief clear all parents and children
     */
    void clear();

    /**
     * @brief checks if the current node and its children are equal a other node and its children
     * @param node the node to compare
     * @return bool
     */
    bool equalWithAllChildren(NodePtr const& node);

    /**
     * @brief checks if the current node and its parents are equal a other node and its parents
     * @param node the node to compare
     * @return bool
     */
    bool equalWithAllParents(NodePtr const& node);

    /**
     * @brief check two node are equal.
     * @param rhs the node to compare
     * @return bool true if they are the same otherwise false
     */
    virtual bool equal(NodePtr const& rhs) const { return this->isIdentical(rhs); };

    /**
     * @brief check two nodes whether are exactly the same object or not
     * @param rhs the node to check
     * @return bool true if they are the same object otherwise false
     */
    [[nodiscard]] virtual bool isIdentical(NodePtr const& rhs) const { return rhs.get() == this; };

    /**
     * @brief split graph into multiple sub-graphs. The graph starts at current splitNode.
     * If the given splitNode is not in the graph, throw exception
     * @params splitNode the given splitNode to split at.
     * @return vector of multiple sub-graphs.
     */
    std::vector<NodePtr> split(NodePtr const& splitNode);

    /**
     * @brief validation of this node
     * @return true if there is no ring/node inside this node's children, otherwise false
     */
    bool isValid();

    /**
     * @brief Checks if the current node is of type NodeType
     * @tparam NodeType
     * @return bool true if node is of NodeType
     */
    template<class NodeType>
    bool instanceOf() {
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
        }
        throw std::logic_error("Node:: we performed an invalid cast of operator " + this->toString() + " to type "
                               + typeid(NodeType).name());
        return nullptr;
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
     * @brief checks if the node and its children contain cycles.
     * @return true if cyclic
     */
    bool isCyclic();

    /**
     * @brief return all children of current node
     *        Always excluding current node, no matter a cycle exists
     * @param withDuplicateChildren: set true to allow to retrieve duplicate elements
     * @return allChildren a vector to store all children of current node
     */
    std::vector<NodePtr> getAndFlattenAllChildren(bool withDuplicateChildren);

    /**
     * @brief get direct children.
     * @return vector of children.
     */
    const std::vector<NodePtr>& getChildren() const;

    /**
     * @brief Check if the node is present as the parent or grand-parent
     * @param node: node to look for
     * @return true if present as false
     */
    bool containAsGrandParent(NodePtr node);

    /**
     * @brief Check if input node is present as parent to this
     * @return true if input node is in parent list
     */
    virtual bool containAsParent(NodePtr node);

    /**
     * @brief Check if the node is present as the child or grand-child
     * @param node: node to look for
     * @return true if present as false
     */
    bool containAsGrandChild(NodePtr node);

    /**
     * @brief Check if input node is present as children to this
     * @return true if input node is in children list
     */
    virtual bool containAsChild(NodePtr node);

    /**
     * @brief get direct parents.
     * @return vector of parents.
     */
    const std::vector<NodePtr>& getParents() const;

    /**
     * Get all the root nodes
     * @return vector of root nodes
     */
    std::vector<NodePtr> getAllRootNodes();

    /**
     * @brief Get all the leaf nodes
     * @return vector of leaf nodes
     */
    std::vector<NodePtr> getAllLeafNodes();

    /**
     * @brief Add input node as parent to the current node and move the parents of current node as parent to the input node.
     * If the node is already exists as parent then skip the operation
     * @return true if operation succeeded else false
     */
    bool insertBetweenThisAndParentNodes(NodePtr const& newNode);

    /**
    * @brief Add input node as child to the current node and add the input node as new parent to the old child
    * @return true if operation succeeded else false
    */
    bool insertBetweenThisAndChildNodes(NodePtr const& newNode);

    /**
     * @brief To string method for the current node.
     * @return string
     */
    [[nodiscard]] virtual std::string toString() const = 0;

    /**
     * @brief To multiline string method for the current node.
     * @return string
     */
    virtual std::vector<std::string> toMultilineString();

    /**
     * @brief check if an nodeToFind is in given graph
     * @param root
     * @param nodeToFind
     * @return return true if the given nodeToFind is found in the graph of root, otherwise false
     */
    NodePtr findRecursively(NodePtr const& root, NodePtr const& nodeToFind);

    /**
     * @brief Get all nodes that are parents to this node.
     * @return vector of all of its parent nodes
     */
    std::vector<NodePtr> getAndFlattenAllAncestors();

    /**
     * @brief Returns stacktrace of node source location.
     * @return string
     */
    std::string getNodeSourceLocation();

  protected:
    /**
     * @brief the parents of this node. There is no equal nodes
     *        in this vector
     */
    std::vector<NodePtr> parents{};
    /**
     * @brief the children of this node. There is no equal nodes
     *        in this vector
     */
    std::vector<NodePtr> children{};

  private:
    /**
    * @brief helper function of getNodeType() function
    */
    template<class NodeType>
    void getNodesByTypeHelper(std::vector<std::shared_ptr<NodeType>>& foundNodes) {
        if (this->instanceOf<NodeType>()) {
            foundNodes.push_back(this->as<NodeType>());
        }
        for (auto& successor : this->children) {
            successor->getNodesByTypeHelper(foundNodes);
        }
    };

    /**
     * @brief check if an node is in given vector or not
     * @param nodes
     * @param nodeToFind
     * @return return true if the given node is found, otherwise false
     */
    bool vectorContainsTheNode(const std::vector<NodePtr>& nodes, NodePtr const& nodeToFind);

    /**
     * @brief check if an node is in given vector and returns it
     * @param nodes
     * @param nodeToFind
     * @return return node if the given node is found, otherwise nullpointer
     */
    static NodePtr find(std::vector<NodePtr> const& nodes, NodePtr const& nodeToFind);

    /********************************************************************************
     *                   Helper functions                                           *
     ********************************************************************************/
    /**
     * @brief helper function of equalWithAllParents() function
     */
    bool equalWithAllParentsHelper(NodePtr const& node1, NodePtr const& node2);
    /**
     * @brief helper function of equalWithAllChildren() function
     */
    bool equalWithAllChildrenHelper(NodePtr const& node1, NodePtr const& node2);

    /**
     * @brief helper function of getAndFlattenAllChildren() function
     */
    void getAndFlattenAllChildrenHelper(NodePtr const& node,
                                        std::vector<NodePtr>& allChildren,
                                        NodePtr const& excludedNode,
                                        bool allowDuplicate);

    /**
     * @brief helper function of cycle detector
     */
    bool isCyclicHelper(Node& node);

    /********************************************************************************
     *                   Helper parameters                                           *
     ********************************************************************************/
    /**
     * @brief Helper parameters for cycle detection
     */
    bool visited{false};
    bool recStack{false};

    /**
     * @brief Stores the stacktrace while node creation.
     * It is empty if CMAKE_NES_TRACE_NODE_CREATION == false.
     */
    std::string stackTrace;
};
}// namespace NES

#endif// NODES_NODE_HPP
