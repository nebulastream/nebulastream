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

#pragma once

#include <algorithm>
#include <memory>
#include <ostream>
#include <sstream>
#include <vector>
#include <Util/Common.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>

namespace NES
{


class Node : public std::enable_shared_from_this<Node>
{
public:
    virtual ~Node() = default;

    /**
     * @brief adds a newNode as a child to the current newNode.
     * @note Duplicates inside the children are not ignored.
     * @note A newNode cannot be in its own child.
     * @param newNode
     */
    bool addChildWithEqual(const std::shared_ptr<Node>& newNode);

    /**
     * @brief adds a newNode as a child to the current newNode.
     * @note Duplicates inside the children are ignored.
     * @note  A newNode cannot be in its own child.
     * @param newNode
     */
    virtual bool addChild(const std::shared_ptr<Node>& newNode);

    /**
     * @brief remove a node from current children.
     * @param node
     */
    bool removeChild(const std::shared_ptr<Node>& node);

    /**
     * @brief add a parent to vector of parents
     *        no duplicated node inside parents.
     *        one cannot add current node into its parents.
     * @param newNode
     */
    virtual bool addParent(const std::shared_ptr<Node>& newNode);

    /**
     * @brief remove a parent from vector of parents
     * @param node
     */
    bool removeParent(const std::shared_ptr<Node>& node);

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
    bool replace(const std::shared_ptr<Node>& newNode, const std::shared_ptr<Node>& oldNode);

    /**
     * @brief replace current node with new node
     * @param newNode
     * @return
     */
    bool replace(const std::shared_ptr<Node>& newNode);

    /**
     * @brief swap given old node by new node
     * @param newNode the node to mount at oldNode parents instead of oldNode
     * @param oldNode the node to remove from graph
     * @return true if swapping successfully otherwise false
     */
    bool swap(const std::shared_ptr<Node>& newNode, const std::shared_ptr<Node>& oldNode);

    /**
     * @brief Swaps the left and right branch of binary nodes, by reversing the vector of children.
     * @return bool true if successful
     */
    bool swapLeftAndRightBranch();

    /**
     * @brief remove the given node together with its children
     * @param node the given node to remove
     * @return bool true if successful
     */
    bool remove(const std::shared_ptr<Node>& node);

    /**
     * @brief remove the given node and add its children to the children of the current node
     * @param node
     * @return bool true if successful
     */
    bool removeAndLevelUpChildren(const std::shared_ptr<Node>& node);

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
    bool equalWithAllChildren(const std::shared_ptr<Node>& node);

    /**
     * @brief checks if the current node and its parents are equal a other node and its parents
     * @param node the node to compare
     * @return bool
     */
    bool equalWithAllParents(const std::shared_ptr<Node>& node);


    /// returns true if there query graph is acyclic
    bool isValid();

    /// Checks if the pointers are equal
    [[nodiscard]] virtual bool isIdentical(const std::shared_ptr<Node>& rhs) const { return rhs.get() == this; };

    /// Wrapper around 'isIdentical'
    virtual bool equal(const std::shared_ptr<Node>& rhs) const { return this->isIdentical(rhs); };

    /// Collects all nodes that are of a specific node type, e.g. all FilterOperatorNodes.
    template <class NodeType>
    std::vector<std::shared_ptr<NodeType>> getNodesByType()
    {
        std::vector<std::shared_ptr<NodeType>> vector;
        getNodesByTypeHelper<NodeType>(vector);
        return vector;
    }

    bool isCyclic();

    /**
     * @brief return all children of current node
     *        Always excluding current node, no matter a cycle exists
     * @param withDuplicateChildren: set true to allow to retrieve duplicate elements
     * @return allChildren a vector to store all children of current node
     */
    std::vector<std::shared_ptr<Node>> getAndFlattenAllChildren(bool withDuplicateChildren);

    /**
     * @brief get direct children.
     * @return vector of children.
     */
    const std::vector<std::shared_ptr<Node>>& getChildren() const;

    /**
     * @brief Check if the node is present as the parent or grand-parent
     * @param node: node to look for
     * @return true if present as false
     */
    virtual bool containAsGrandParent(const std::shared_ptr<Node>& node);

    /**
     * @brief Check if input node is present as parent to this
     * @return true if input node is in parent list
     */
    virtual bool containAsParent(const std::shared_ptr<Node>& node);

    /**
     * @brief Check if the node is present as the child or grand-child
     * @param node: node to look for
     * @return true if present as false
     */
    virtual bool containAsGrandChild(const std::shared_ptr<Node>& node);

    /**
     * @brief Check if input node is present as children to this
     * @return true if input node is in children list
     */
    virtual bool containAsChild(const std::shared_ptr<Node>& node);

    /**
     * @brief get direct parents.
     * @return vector of parents.
     */
    const std::vector<std::shared_ptr<Node>>& getParents() const;

    /**
     * Get all the root nodes
     * @return vector of root nodes
     */
    std::vector<std::shared_ptr<Node>> getAllRootNodes();

    /**
     * @brief Get all the leaf nodes
     * @return vector of leaf nodes
     */
    std::vector<std::shared_ptr<Node>> getAllLeafNodes();

    /**
     * @brief Add input node as parent to the current node and move the parents of current node as parent to the input node.
     * If the node is already exists as parent then skip the operation
     * @return true if operation succeeded else false
     */
    bool insertBetweenThisAndParentNodes(const std::shared_ptr<Node>& newNode);

    /**
    * @brief Add input node as child to the current node and add the input node as new parent to the old child
    * @return true if operation succeeded else false
    */
    bool insertBetweenThisAndChildNodes(const std::shared_ptr<Node>& newNode);

    /**
     * @brief check if a node is the child or grandchild of the given root node
     * @param root the root node
     * @param nodeToFind the node to find
     * @return return true if the given nodeToFind is found in the graph of root, otherwise false
     */
    static std::shared_ptr<Node> findRecursively(const std::shared_ptr<Node>& root, const std::shared_ptr<Node>& nodeToFind);

    /**
     * @brief Get all nodes that are parents to this node.
     * @return vector of all of its parent nodes
     */
    std::vector<std::shared_ptr<Node>> getAndFlattenAllAncestors();

    friend std::ostream& operator<<(std::ostream& os, const Node& node);

protected:
    /**
     * @brief the parents of this node. There is no equal nodes
     *        in this vector
     */
    std::vector<std::shared_ptr<Node>> parents;
    /**
     * @brief the children of this node. There is no equal nodes
     *        in this vector
     */
    std::vector<std::shared_ptr<Node>> children;

    /// The usual string representation of the current node. Used by `operator<<`. Includes as much information about the node as possible.
    [[nodiscard]] virtual std::ostream& toDebugString(std::ostream& os) const = 0;

    /// Returns a shorter string representation for the current node. Accessed by setting the state of the current stream to
    /// `VerbosityLevel::QueryPlan` (`ss << VerbosityLevel::QueryPlan << node`).
    [[nodiscard]] virtual std::ostream& toQueryPlanString(std::ostream& os) const
    {
        os << VerbosityLevel::QueryPlan;
        return toDebugString(os);
    }

private:
    /**
    * @brief helper function of getSpatialType() function
    */
    template <class NodeType>
    void getNodesByTypeHelper(std::vector<std::shared_ptr<NodeType>>& foundNodes)
    {
        auto sharedThis = this->shared_from_this();
        if (NES::Util::instanceOf<NodeType>(sharedThis))
        {
            foundNodes.push_back(NES::Util::as<NodeType>(sharedThis));
        }
        for (auto& successor : this->children)
        {
            successor->getNodesByTypeHelper(foundNodes);
        }
    };

    /**
     * @brief check if an node is in given vector or not
     * @param nodes
     * @param nodeToFind
     * @return return true if the given node is found, otherwise false
     */
    static bool vectorContainsTheNode(const std::vector<std::shared_ptr<Node>>& nodes, const std::shared_ptr<Node>& nodeToFind);

    /**
     * @brief check if an node is in given vector and returns it
     * @param nodes
     * @param nodeToFind
     * @return return node if the given node is found, otherwise nullpointer
     */
    static std::shared_ptr<Node> find(const std::vector<std::shared_ptr<Node>>& nodes, const std::shared_ptr<Node>& nodeToFind);

    /********************************************************************************
     *                   Helper functions                                           *
     ********************************************************************************/
    /**
     * @brief helper function of equalWithAllParents() function
     */
    bool equalWithAllParentsHelper(const std::shared_ptr<Node>& node1, const std::shared_ptr<Node>& node2);
    /**
     * @brief helper function of equalWithAllChildren() function
     */
    bool equalWithAllChildrenHelper(const std::shared_ptr<Node>& node1, const std::shared_ptr<Node>& node2);

    /**
     * @brief helper function of getAndFlattenAllChildren() function
     */
    void getAndFlattenAllChildrenHelper(
        const std::shared_ptr<Node>& node,
        std::vector<std::shared_ptr<Node>>& allChildren,
        const std::shared_ptr<Node>& excludedNode,
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
};

inline std::ostream& operator<<(std::ostream& os, const Node& node)
{
    switch (getVerbosityLevel(os))
    {
        case VerbosityLevel::Debug:
        default:
            return node.toDebugString(os);
            break;
        case VerbosityLevel::QueryPlan:
            return node.toQueryPlanString(os);
            break;
    }
}
}

namespace fmt
{
/// Implements formatting via fmt::format("{}") for all classes inheriting from Node. Internally, the `operator<<`
/// implementation is used.
/// By putting a 'q' behind the ':' in the format string, you can print the Node with the `VerbosityLevel` `QueryPlan`:
/// ```cpp
/// auto _ = fmt::format("Node string with less noise: {:q}", *node);
/// ```
/// Any other characters behind the ':' are not supported and are ignored.
template <typename T>
requires(std::is_base_of_v<NES::Node, T>)
struct formatter<T> : ostream_formatter
{
    /// This character changes the default `VerbosityLevel` from `Debug` to `QueryPlan`.
    char queryPlanOptionCharacter = 'q';
    bool isQueryPlan{false};

    constexpr auto parse(fmt::format_parse_context& ctx)
    {
        /// Relies on std::ranges::find to find the first occurrence from the left.
        const auto* pos = std::ranges::find(ctx, '}');
        if (pos != ctx.end())
        {
            /// While it seems like ctx ends with `}`, the documentation states that it also contains everything afterwards. To be sure, we use pos to find `}`.
            if (std::ranges::find(ctx.begin(), pos, queryPlanOptionCharacter) != ctx.end())
            {
                isQueryPlan = true;
            }
        }
        return pos;
    }

    auto format(const T& node, format_context& ctx) const
    {
        if (isQueryPlan)
        {
            /// Look at the implementation of ostream_formatter in fmt/ostream to make this more efficient.
            std::stringstream ss;
            ss << NES::VerbosityLevel::QueryPlan;
            ss << node;
            return ostream_formatter::format(ss.str(), ctx);
        }
        return ostream_formatter::format(node, ctx);
    }
};
}
