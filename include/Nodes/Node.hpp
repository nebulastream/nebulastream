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
typedef std::shared_ptr<Node> NodePtr;

class Node : public std::enable_shared_from_this<Node> {
  public:
    Node();
    ~Node();

    /**
     * @brief adds a newNode as a child to the current newNode.
     * @note Duplicates inside the children are not ignored.
     * @note A newNode cannot be in its own child.
     * @param newNode
     */
    bool addChildWithEqual(const NodePtr newNode);

    /**
     * @brief adds a newNode as a child to the current newNode.
     * @note Duplicates inside the children are ignored.
     * @note  A newNode cannot be in its own child.
     * @param newNode
     */
    virtual bool addChild(const NodePtr newNode);

    /**
     * @brief remove a node from current children.
     * @param node
     */
    bool removeChild(const NodePtr node);

    /**
     * @brief add a parent to vector of parents
     *        no duplicated node inside parents.
     *        one cannot add current node into its parents.
     * @param newNode
     */
    virtual bool addParent(const NodePtr newNode);

    /**
     * @brief remove a parent from vector of parents
     * @param node
     */
    bool removeParent(const NodePtr node);

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
     *    children. If old node has children, merge the children of old nodes
     *    and new nodes's children. If there's duplicated children among old nodes and
     *    new nodes's children, the children in new noeds will overwrite that
     *    inside old noeds's.
     * 2)
     * @param newNode
     * @param oldNode
     */
    bool replace(NodePtr newNode, NodePtr oldNode);

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
    bool swap(const NodePtr newNode, const NodePtr oldNode);

    /**
     * @brief remove the given node together with its children
     * @param node the given node to remove
     * @return bool true if successful
     */
    bool remove(const NodePtr node);

    /**
     * @brief remove the given node and add its children to the children of the current node
     * @param node
     * @return bool true if successful
     */
    bool removeAndLevelUpChildren(const NodePtr node);

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
    bool equalWithAllChildren(const NodePtr node);

    /**
     * @brief checks if the current node and its parents are equal a other node and its parents
     * @param node the node to compare
     * @return bool
     */
    bool equalWithAllParents(const NodePtr node);

    /**
     * @brief check two node are equal.
     * @param rhs the node to compare
     * @return bool true if they are the same otherwise false
     */
    virtual bool equal(const NodePtr rhs) const { return this->isIdentical(rhs); };

    /**
     * @brief check two nodes whether are exactly the same object or not
     * @param rhs the node to check
     * @return bool true if they are the same object otherwise false
     */
    virtual bool isIdentical(const NodePtr rhs) const { return rhs.get() == this; };

    /**
     * @brief split graph into multiple sub-graphs. The graph starts at current splitNode.
     * If the given splitNode is not in the graph, throw exception
     * @params splitNode the given splitNode to split at.
     * @return vector of multiple sub-graphs.
     */
    std::vector<NodePtr> split(const NodePtr splitNode);

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
        } else {
            NES_THROW_RUNTIME_ERROR("Node:: we performed an invalid cast of operator " + this->toString() + " to type "
                                    + typeid(NodeType).name());
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
     * @brief checks if the node and its children contain cycles.
     * @return true if cyclic
     */
    bool isCyclic();

    /**
     * @brief return all children of current node
     *        Always excluding current node, no matter a cycle exists
     * @params allChildren a vector to store all children of current node
     */
    std::vector<NodePtr> getAndFlattenAllChildren();

    /**
     * @brief get direct children.
     * @return vector of children.
     */
    const std::vector<NodePtr>& getChildren() const;

    /**
     * Check if input node is present as parent to this
     * @return true if input node is in parent list
     */
    virtual bool containAsParent(NodePtr node);

    /**
     * Check if input node is present as children to this
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
     * Get all the leaf nodes
     * @return vector of leaf nodes
     */
    std::vector<NodePtr> getAllLeafNodes();

    /**
     * @brief Add input node as parent to the current node and move the parents of current node as parent to the input node.
     * If the node is already exists as parent then skip the operation
     * @return true if operation succeeded else false
     */
    bool insertBetweenThisAndParentNodes(const NodePtr newNode);

    /**
    * @brief Add input node as child to the current node and move the parents of current node as parent to the input node.
    * If the node is already exists as parent then skip the operation
    * @return true if operation succeeded else false
    */
    bool insertBetweenThisAndChildNodes(const NodePtr newNode);

    /**
     * @brief To string method for the current node.
     * @return string
     */
    virtual const std::string toString() const = 0;

    /**
     * @brief To multiline string method for the current node.
     * @return string
     */
    virtual const std::vector<std::string> toMultilineString();

    /**
     * @brief check if an nodeToFind is in given graph
     * @param root
     * @param nodeToFind
     * @return return true if the given nodeToFind is found in the graph of root, otherwise false
     */
    NodePtr findRecursively(const NodePtr root, const NodePtr nodeToFind);

    /**
     * @brief Get all nodes that are parents to this node.
     * @return vector of all of its parent nodes
     */
    std::vector<NodePtr> getAndFlattenAllAncestors();

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
    bool vectorContainsTheNode(const std::vector<NodePtr>& nodes, const NodePtr nodeToFind);

    /**
     * @brief check if an node is in given vector and returns it
     * @param nodes
     * @param nodeToFind
     * @return return node if the given node is found, otherwise nullpointer
     */
    NodePtr find(const std::vector<NodePtr>& nodes, const NodePtr nodeToFind);

    /********************************************************************************
     *                   Helper functions                                           *
     ********************************************************************************/
    /**
     * @brief helper function of equalWithAllParents() function
     */
    bool equalWithAllParentsHelper(const NodePtr node1, const NodePtr node2);
    /**
     * @brief helper function of equalWithAllChildren() function
     */
    bool equalWithAllChildrenHelper(const NodePtr node1, const NodePtr node2);

    /**
     * @brief helper function of getAndFlattenAllChildren() function
     */
    void getAndFlattenAllChildrenHelper(const NodePtr node, std::vector<NodePtr>& allChildren, const NodePtr excludednode);

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

#endif// NODES_NODE_HPP
