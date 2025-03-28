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

#include <any>
#include <memory>
#include <ostream>
#include <string>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Common.hpp>
#include <fmt/base.h>

namespace NES
{

class Schema;

/// Returns the next free operator id
OperatorId getNextOperatorId();

class Operator : public std::enable_shared_from_this<Operator>
{
public:
    Operator() : id(getNextOperatorId()) {}; // New option to just create it without Id. Needed for the physical operators
    explicit Operator(OperatorId id);
    virtual ~Operator() noexcept = default;

    OperatorId getId() const;
    void setId(OperatorId id);

    /// Create duplicate of this operator by copying its context information and also its parent and child operator set.
    std::shared_ptr<Operator> duplicate();

    virtual std::shared_ptr<Operator> clone() const = 0;

    /// Are the operators equal. Does not check for equal id
    virtual bool operator==(const Operator& rhs) const = 0;
    /// Are the operators equal and share the same id
    virtual bool isIdentical(const Operator& rhs) const = 0;

    bool hasMultipleChildrenOrParents() const;
    bool hasMultipleChildren() const;
    bool hasMultipleParents() const;

    bool addChild(std::shared_ptr<Operator> newNode);
    bool addParent(std::shared_ptr<Operator> newNode);

    std::shared_ptr<Operator> getChildWithOperatorId(OperatorId operatorId) const;

    bool containAsGrandChild(std::shared_ptr<Operator> operatorNode);
    bool containAsGrandParent(std::shared_ptr<Operator> operatorNode);

    std::shared_ptr<Schema> getOutputSchema() const;
    void setOutputSchema(std::shared_ptr<Schema> outputSchema);

    virtual std::vector<OriginId> getOutputOriginIds() const = 0;

    template <class OperatorType>
    void getOperatorByTypeHelper(std::vector<std::shared_ptr<OperatorType>>& foundNodes)
    {
        auto sharedThis = this->shared_from_this();
        if (NES::Util::instanceOf<OperatorType>(sharedThis))
        {
            foundNodes.push_back(NES::Util::as<OperatorType>(sharedThis));
        }
        for (auto& successor : this->children)
        {
            successor->getOperatorByTypeHelper(foundNodes);
        }
    };

    template <class OperatorType>
    std::vector<std::shared_ptr<OperatorType>> getOperatorByType()
    {
        std::vector<std::shared_ptr<OperatorType>> vector;
        getOperatorByTypeHelper<OperatorType>(vector);
        return vector;
    }

    bool insertBetweenThisAndChildNodes(const std::shared_ptr<Operator>& newNode)
    {
        if (newNode.get() == this)
        {
            NES_WARNING("Node:  Adding node to its self so will skip insertBetweenThisAndParentNodes operation.");
            return false;
        }

        if (vectorContainsTheNode(children, newNode))
        {
            NES_WARNING("Node: the node is already part of its parents so ignore insertBetweenThisAndParentNodes operation.");
            return false;
        }

        NES_INFO("Node: Create temporary copy of this nodes parents.");
        std::vector<std::shared_ptr<Operator>> copyOfChildren = children;

        NES_INFO("Node: Remove all children of this node.");
        removeChildren();

        if (!addChild(newNode))
        {
            NES_ERROR("Node: Unable to add input node as parent to this node.");
            return false;
        }

        NES_INFO("Node: Add copy of this nodes parent as parent to the input node.");
        for (const std::shared_ptr<Operator>& child : copyOfChildren)
        {
            if (!newNode->addChild(child))
            {
                NES_ERROR("Node: Unable to add child of this node as child to input node.");
                return false;
            }
        }

        return true;
    }


    std::shared_ptr<Operator> find(const std::vector<std::shared_ptr<Operator>>& nodes, const std::shared_ptr<Operator>& nodeToFind)
    {
        for (auto&& currentNode : nodes)
        {
            if (nodeToFind == currentNode)
            { /// TODO: need to check this when merge is used. nodeToFind.get() == currentNode.get()
                return currentNode;
            }
        }
        return nullptr;
    }

    void removeAllParent()
    {
        NES_INFO("Node: Removing all parents for current node");
        auto nodeItr = parents.begin();
        while (nodeItr != parents.end())
        {
            if (!this->removeParent(*nodeItr))
            {
                nodeItr++;
            }
            NES_INFO("Node: Removed node as parent of this node");
        }
    }

    bool vectorContainsTheNode(std::vector<std::shared_ptr<Operator>> const& nodes, std::shared_ptr<Operator> const& nodeToFind)
    {
        return find(nodes, nodeToFind) != nullptr;
    }

    bool removeParent(std::shared_ptr<Operator> const& node)
    {
        if (!node)
        {
            NES_ERROR("Node: Can't remove null node");
            return false;
        }

        /// check all parents.
        for (auto nodeItr = parents.begin(); nodeItr != parents.end(); ++nodeItr)
        {
            if ((*nodeItr).get() == node.get())
            {
                for (auto it = (*nodeItr)->children.begin(); it != (*nodeItr)->children.end(); it++)
                {
                    if ((*it).get() == this)
                    {
                        (*nodeItr)->children.erase(it);
                        break;
                    }
                }
                parents.erase(nodeItr);
                return true;
            }
        }
        NES_DEBUG("Node: node was not found and could not be removed from parents.");
        return false;
    }


    bool insertBetweenThisAndParentNodes(std::shared_ptr<Operator> const& newNode)
    {
        ///Perform sanity checks
        if (newNode.get() == this)
        {
            NES_WARNING("Node:  Adding node to its self so will skip insertBetweenThisAndParentNodes operation.");
            return false;
        }

        if (vectorContainsTheNode(parents, newNode))
        {
            NES_WARNING("Node: the node is already part of its parents so ignore insertBetweenThisAndParentNodes operation.");
            return false;
        }

        ///replace this with the new node in all its parent
        NES_DEBUG("Node: Create temporary copy of this nodes parents.");
        std::vector<std::shared_ptr<Operator>> copyOfParents = parents;

        for (auto& parent : copyOfParents)
        {
            for (uint64_t i = 0; i < parent->children.size(); i++)
            {
                if (parent->children[i] == shared_from_this())
                {
                    parent->children[i] = newNode;
                    NES_DEBUG("Node: Add copy of this nodes parent as parent to the input node.");
                    if (!newNode->addParent(parent))
                    {
                        NES_ERROR("Node: Unable to add parent of this node as parent to input node.");
                        return false;
                    }
                }
            }
        }

        NES_INFO("Node: Remove all parents of this node.");
        removeAllParent();

        if (!addParent(newNode))
        {
            NES_ERROR("Node: Unable to add input node as parent to this node.");
            return false;
        }
        return true;
    }

    bool removeChild(std::shared_ptr<Operator> const& node)
    {
        if (!node)
        {
            NES_ERROR("Node: Can't remove null node");
            return false;
        }

        /// check all children.
        for (auto nodeItr = children.begin(); nodeItr != children.end(); ++nodeItr)
        {
            if ((*nodeItr).get() == node.get())
            {
                /// remove this from nodeItr's parents
                for (auto it = (*nodeItr)->parents.begin(); it != (*nodeItr)->parents.end(); it++)
                {
                    if ((*it).get() == this)
                    {
                        (*nodeItr)->parents.erase(it);
                        break;
                    }
                }
                /// remove nodeItr from children
                children.erase(nodeItr);
                return true;
            }
        }
        NES_DEBUG("Node: node was not found and could not be removed from children.");
        return false;
    }

    void removeChildren()
    {
        NES_INFO("Node: Removing all children for current node");
        auto nodeItr = children.begin();
        while (nodeItr != children.end())
        {
            if (!this->removeChild(*nodeItr))
            {
                nodeItr++;
            }
            NES_INFO("Node: Removed node as child of this node");
        }
    }

    bool removeAndJoinParentAndChildren()
    {
        try
        {
            NES_DEBUG("Node: Joining parents with children");

            std::vector<std::shared_ptr<Operator>> childCopy = this->children;
            std::vector<std::shared_ptr<Operator>> parentCopy = this->parents;
            for (auto& parent : parentCopy)
            {
                for (auto& child : childCopy)
                {
                    NES_DEBUG("Node: Add child of this node as child of this node's parent");
                    parent->addChild(child);

                    NES_DEBUG("Node: remove this node as parent of the child");
                    child->removeParent(shared_from_this());
                }
                parent->removeChild(shared_from_this());
                NES_DEBUG("Node: remove this node as child of this node's parents");
            }
            return true;
        }
        catch (...)
        {
            NES_ERROR("Node: Error occurred while joining this node's children and parents");
            return false;
        }
    }

    bool replace(std::shared_ptr<Operator> other)
    {
        return replace(std::move(other), shared_from_this());
    }

    bool replace(const std::shared_ptr<Operator>& newNode, const std::shared_ptr<Operator>& oldNode)
    {
        if (!newNode || !oldNode)
        {
            return false;
        }

        if (shared_from_this() == oldNode)
        {
            insertBetweenThisAndParentNodes(newNode);
            removeAndJoinParentAndChildren();
            return true;
        }

        if (newNode && oldNode->isIdentical(*newNode))
        {
            NES_WARNING("Node: the new node was the same so will skip replace operation.");
            return true;
        }

        if (oldNode != newNode)
        {
            /// newNode is already inside children or parents and it's not oldNode
            if (find(children, newNode) || find(parents, newNode))
            {
                NES_DEBUG("Node: the new node is already part of the children or predecessors of the current node.");
                return false;
            }
        }

        bool success = removeChild(oldNode);
        if (success)
        {
            children.push_back(newNode);
            for (auto&& currentNode : oldNode->children)
            {
                newNode->addChild(currentNode);
            }
            return true;
        }
        NES_ERROR("Node: could not remove child from  old node: {}", oldNode->toString());

        success = removeParent(oldNode);
        NES_DEBUG("Node: remove parent old node: {}", oldNode->toString());
        if (success)
        {
            parents.push_back(newNode);
            for (auto&& currentNode : oldNode->parents)
            {
                newNode->addParent(currentNode);
            }
            return true;
        }
        NES_ERROR("Node: could not remove parent from  old node: {}", oldNode->toString());

        return false;
    }

    std::vector<std::shared_ptr<Operator>> getAndFlattenAllChildren(bool withDuplicateChildren);

    template <class OperatorType>
    std::vector<std::shared_ptr<OperatorType>> getOperatorsByType()
    {
        std::vector<std::shared_ptr<OperatorType>> vector;
        getOperatorsByTypeHelper<OperatorType>(vector);
        return vector;
    }

    friend std::ostream& operator<<(std::ostream& os, const Operator& node);

    [[nodiscard]] virtual std::string toString() const = 0;

    std::vector<std::shared_ptr<Operator>> getParents() const
    {
        return parents;
    }

    std::vector<std::shared_ptr<Operator>> getChildren() const
    {
        return children;
    }

protected:
    std::vector<std::shared_ptr<Operator>> children;
    std::vector<std::shared_ptr<Operator>> parents;

private:
    template <class OperatorType>
    void getOperatorsByTypeHelper(std::vector<std::shared_ptr<OperatorType>>& foundNodes)
    {
        auto sharedThis = this->shared_from_this();
        if (NES::Util::instanceOf<OperatorType>(sharedThis))
        {
            foundNodes.push_back(NES::Util::as<OperatorType>(sharedThis));
        }
        for (auto& successor : this->children)
        {
            successor->getOperatorsByTypeHelper(foundNodes);
        }
    };

protected:
    std::shared_ptr<Operator> getDuplicateOfParent(const std::shared_ptr<Operator>& operatorNode);
    std::shared_ptr<Operator> getDuplicateOfChild(const std::shared_ptr<Operator>& operatorNode);

    std::shared_ptr<Schema> outputSchema;
    OperatorId id;
};

inline std::ostream& operator<<(std::ostream& os, const Operator& node)
{
    return os << node.toString();
}
}

template <typename T>
requires(std::is_base_of_v<NES::Operator, T>)
struct fmt::formatter<T> : fmt::ostream_formatter
{
};
