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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/BatchJoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Util/UtilityFunctions.hpp>
#include <algorithm>
#include <utility>

namespace NES {
/**
 * @brief We initialize the input and output schemas with empty schemas.
 */
OperatorNode::OperatorNode(OperatorId id) : id(id), properties() { NES_INFO("Creating Operator " << id); }

OperatorId OperatorNode::getId() const { return id; }

void OperatorNode::setId(OperatorId id) { OperatorNode::id = id; }

bool OperatorNode::hasMultipleChildrenOrParents() {
    //has multiple child operator
    bool hasMultipleChildren = (!getChildren().empty()) && getChildren().size() > 1;
    //has multiple parent operator
    bool hasMultipleParent = (!getParents().empty()) && getParents().size() > 1;
    NES_DEBUG("OperatorNode: has multiple children " << hasMultipleChildren << " or has multiple parent " << hasMultipleParent);
    return hasMultipleChildren || hasMultipleParent;
}

OperatorNodePtr OperatorNode::duplicate() {
    NES_INFO("OperatorNode: Create copy of the operator");
    const OperatorNodePtr copyOperator = copy();

    NES_DEBUG("OperatorNode: copy all parents");
    for (const auto& parent : getParents()) {
        if (!copyOperator->addParent(getDuplicateOfParent(parent->as<OperatorNode>()))) {
            NES_THROW_RUNTIME_ERROR("OperatorNode: Unable to add parent to copy");
        }
    }

    NES_DEBUG("OperatorNode: copy all children");
    for (const auto& child : getChildren()) {
        if (!copyOperator->addChild(getDuplicateOfChild(child->as<OperatorNode>()->duplicate()))) {
            NES_THROW_RUNTIME_ERROR("OperatorNode: Unable to add child to copy");
        }
    }
    return copyOperator;
}

OperatorNodePtr OperatorNode::getDuplicateOfParent(const OperatorNodePtr& operatorNode) {
    NES_DEBUG("OperatorNode: create copy of the input operator");
    const OperatorNodePtr& copyOfOperator = operatorNode->copy();
    if (operatorNode->getParents().empty()) {
        NES_TRACE("OperatorNode: No ancestor of the input node. Returning the copy of the input operator");
        return copyOfOperator;
    }

    NES_TRACE("OperatorNode: For all parents get copy of the ancestor and add as parent to the copy of the input operator");
    for (const auto& parent : operatorNode->getParents()) {
        copyOfOperator->addParent(getDuplicateOfParent(parent->as<OperatorNode>()));
    }
    NES_TRACE("OperatorNode: return copy of the input operator");
    return copyOfOperator;
}

OperatorNodePtr OperatorNode::getDuplicateOfChild(const OperatorNodePtr& operatorNode) {
    NES_DEBUG("OperatorNode: create copy of the input operator");
    OperatorNodePtr copyOfOperator = operatorNode->copy();
    if (operatorNode->getChildren().empty()) {
        NES_TRACE("OperatorNode: No children of the input node. Returning the copy of the input operator");
        return copyOfOperator;
    }

    NES_TRACE("OperatorNode: For all children get copy of their children and add as child to the copy of the input operator");
    for (const auto& child : operatorNode->getChildren()) {
        copyOfOperator->addChild(getDuplicateOfParent(child->as<OperatorNode>()));
    }
    NES_TRACE("OperatorNode: return copy of the input operator");
    return copyOfOperator;
}

bool OperatorNode::addChild(NodePtr newNode) {

    if (!newNode) {
        NES_ERROR("OperatorNode: Can't add null node");
        return false;
    }

    if (newNode->as<OperatorNode>()->getId() == id) {
        NES_ERROR("OperatorNode: can not add self as child to itself");
        return false;
    }

    std::vector<NodePtr> currentChildren = getChildren();
    auto found = std::find_if(currentChildren.begin(), currentChildren.end(), [&](const NodePtr& child) {
        return child->as<OperatorNode>()->getId() == newNode->as<OperatorNode>()->getId();
    });

    if (found == currentChildren.end()) {
        NES_DEBUG("OperatorNode: Adding node to the children.");
        children.push_back(newNode);
        newNode->addParent(shared_from_this());
        return true;
    }
    NES_DEBUG("OperatorNode: the node is already part of its children so skip add child operation.");
    return false;
}

bool OperatorNode::addParent(NodePtr newNode) {

    if (!newNode) {
        NES_ERROR("OperatorNode: Can't add null node");
        return false;
    }

    if (newNode->as<OperatorNode>()->getId() == id) {
        NES_ERROR("OperatorNode: can not add self as parent to itself");
        return false;
    }

    std::vector<NodePtr> currentParents = getParents();
    auto found = std::find_if(currentParents.begin(), currentParents.end(), [&](const NodePtr& child) {
        return child->as<OperatorNode>()->getId() == newNode->as<OperatorNode>()->getId();
    });

    if (found == currentParents.end()) {
        NES_DEBUG("OperatorNode: Adding node to the Parents.");
        parents.push_back(newNode);
        newNode->addChild(shared_from_this());
        return true;
    }
    NES_DEBUG("OperatorNode: the node is already part of its parent so skip add parent operation.");
    return false;
}

bool OperatorNode::removeAndJoinParentAndChildren() {
    try {
        NES_DEBUG("OperatorNode: Joining parents with children for operator " + this->toString());

        if (this->instanceOf<JoinLogicalOperatorNode>() || this->instanceOf<Experimental::BatchJoinLogicalOperatorNode>()) {
            NES_ERROR("OperatorNode: Can not remove a join operator and attach its children to the downstream operator");
            return false;
        }

        std::vector<NodePtr> childCopy = this->children;
        std::vector<NodePtr> parentCopy = this->parents;

        for (auto& parent : parentCopy) {

            //If parent is a logical binary operator then maintain the left and the right operator ids
            if (parent->instanceOf<LogicalBinaryOperatorNode>()) {

                auto logicalBinaryOperator = parent->as<LogicalBinaryOperatorNode>();
                bool isRightUpStreamOperator = logicalBinaryOperator->isRightUpstreamOperatorId(id);
                bool isLeftUpStreamOperator = logicalBinaryOperator->isLeftUpstreamOperatorId(id);

                //Assertions to make sure the operator was in one of the two lists (xor)
                NES_ASSERT(isLeftUpStreamOperator != isRightUpStreamOperator,
                           "An Operator must be and can only be either the left or right upstream operator.");

                for (auto& child : childCopy) {

                    NES_TRACE("OperatorNode: Add child of this node as child of this node's parent");
                    parent->addChild(child);

                    if (isRightUpStreamOperator) {
                        logicalBinaryOperator->addRightUpStreamOperatorId(child->as<LogicalOperatorNode>()->getId());
                    } else {
                        logicalBinaryOperator->addLeftUpStreamOperatorId(child->as<LogicalOperatorNode>()->getId());
                    }

                    NES_TRACE("OperatorNode: remove this node as parent of the child");
                    child->removeParent(shared_from_this());
                }

                if (isRightUpStreamOperator) {
                    logicalBinaryOperator->removeRightUpstreamOperatorId(id);
                } else {
                    logicalBinaryOperator->removeLeftUpstreamOperatorId(id);
                }

            } else {
                for (auto& child : childCopy) {

                    NES_TRACE("OperatorNode: Add child of this node as child of this node's parent");
                    parent->addChild(child);

                    NES_TRACE("OperatorNode: remove this node as parent of the child");
                    child->removeParent(shared_from_this());
                }
            }
            parent->removeChild(shared_from_this());
            NES_DEBUG("OperatorNode: remove this operator as upstream of its downstream operator");
        }
        return true;
    } catch (...) {
        NES_ERROR("OperatorNode: Error occurred while joining this operator's upstream and downstream operators");
        return false;
    }
}

bool OperatorNode::insertBetweenThisAndParentNodes(const OperatorNodePtr& newOperator) {

    if (newOperator->instanceOf<LogicalBinaryOperatorNode>()) {
        NES_ERROR("OperatorNode: Can not insert a binary operator between two operators. We do not have logic to define left or "
                  "the right side.");
        return false;
    }

    //Perform sanity checks
    if (newOperator.get() == this) {
        NES_WARNING("Node:  Adding node to its self so will skip insertBetweenThisAndParentNodes operation.");
        return false;
    }

    if (find(parents, newOperator)) {
        NES_WARNING("Node: the node is already part of its parents so ignore insertBetweenThisAndParentNodes operation.");
        return false;
    }

    //replace this with the new node in all its parent
    NES_DEBUG("Node: Create temporary copy of this nodes parents.");
    std::vector<NodePtr> copyOfParents = parents;
    for (auto& parent : copyOfParents) {

        if (parent->instanceOf<LogicalBinaryOperatorNode>()) {

            auto logicalBinaryOperator = parent->as<LogicalBinaryOperatorNode>();
            bool isRightUpStreamOperator = logicalBinaryOperator->isRightUpstreamOperatorId(id);
            bool isLeftUpStreamOperator = logicalBinaryOperator->isLeftUpstreamOperatorId(id);

            //Assertions to make sure the operator was in one of the two lists (xor)
            NES_ASSERT(isLeftUpStreamOperator != isRightUpStreamOperator,
                       "An Operator must be and can only be either the left or right upstream operator.");

            auto grandChildren = parent->getChildren();
            for (uint64_t i = 0; i < grandChildren.size(); i++) {

                if (grandChildren[i] == shared_from_this()) {
                    grandChildren[i] = newOperator;
                    NES_TRACE("Node: Add copy of this nodes parent as parent to the input node.");
                    if (!newOperator->addParent(parent)) {
                        NES_ERROR("Node: Unable to add parent of this node as parent to input node.");
                        return false;
                    }

                    if (isRightUpStreamOperator) {
                        logicalBinaryOperator->addRightUpStreamOperatorId(newOperator->getId());
                    } else {
                        logicalBinaryOperator->addLeftUpStreamOperatorId(newOperator->getId());
                    }
                }
            }

            //Remove id of this operator from the list
            if (isRightUpStreamOperator) {
                logicalBinaryOperator->removeRightUpstreamOperatorId(id);
            } else {
                logicalBinaryOperator->removeLeftUpstreamOperatorId(id);
            }

        } else {
            auto grandChildren = parent->getChildren();
            for (uint64_t i = 0; i < grandChildren.size(); i++) {
                if (grandChildren[i] == shared_from_this()) {
                    grandChildren[i] = newOperator;
                    NES_TRACE("Node: Add copy of this nodes parent as parent to the input node.");
                    if (!newOperator->addParent(parent)) {
                        NES_ERROR("Node: Unable to add parent of this node as parent to input node.");
                        return false;
                    }
                }
            }
        }

        parent->removeChild(shared_from_this());
    }

    if (!addParent(newOperator)) {
        NES_ERROR("Node: Unable to add input node as parent to this node.");
        return false;
    }
    return true;
}

bool OperatorNode::insertBetweenThisAndChildNodes(const OperatorNodePtr& newNode) {

    if (newNode.get() == this) {
        NES_WARNING("Node:  Adding node to its self so will skip insertBetweenThisAndParentNodes operation.");
        return false;
    }

    if (find(children, newNode)) {
        NES_WARNING("Node: the node is already part of its parents so ignore insertBetweenThisAndParentNodes operation.");
        return false;
    }

    NES_INFO("Node: Create temporary copy of this nodes parents.");
    std::vector<NodePtr> copyOfChildren = children;

    NES_INFO("Node: Remove all childs of this node.");
    removeChildren();

    if (!addChild(newNode)) {
        NES_ERROR("Node: Unable to add input node as parent to this node.");
        return false;
    }

    NES_INFO("Node: Add copy of this nodes parent as parent to the input node.");
    for (const NodePtr& child : copyOfChildren) {
        if (!newNode->addChild(child)) {
            NES_ERROR("Node: Unable to add child of this node as child to input node.");
            return false;
        }
    }

    return true;
}

bool OperatorNode::replace(const OperatorNodePtr& newOperator) {
    return replace(newOperator, shared_from_this()->as<OperatorNode>());
}

bool OperatorNode::replace(const OperatorNodePtr& newOperator, const OperatorNodePtr& oldOperator) {

    if (!newOperator || !oldOperator) {
        NES_ERROR("Node: Can't replace null node");
        return false;
    }

    if (shared_from_this() == oldOperator) {
        insertBetweenThisAndParentNodes(newOperator);
        removeAndJoinParentAndChildren();
        return true;
    }

    if (oldOperator->isIdentical(newOperator)) {
        NES_WARNING("Node: the new node was the same so will skip replace operation.");
        return true;
    }

    //new Operator is not
    if (!oldOperator->equal(newOperator)) {
        // newNode is already inside children or parents and it's not oldNode
        if (find(children, newOperator) || find(parents, newOperator)) {
            NES_DEBUG("Node: the new node is already part of the children or predessessors of the current node.");
            return false;
        }
    }

    bool success = removeChild(oldOperator);
    if (success) {
        children.push_back(newOperator);
        for (auto&& currentNode : oldOperator->getChildren()) {
            newOperator->addChild(currentNode);
        }
        return true;
    }
    NES_ERROR("Node: could not remove child from  old node:" << oldOperator->toString());

    success = removeParent(oldOperator);
    NES_DEBUG("Node: remove parent old node:" << oldOperator->toString());
    if (success) {
        parents.push_back(newOperator);
        for (auto&& currentNode : oldOperator->getParents()) {
            newOperator->addParent(currentNode);
        }
        return true;//TODO: I think this is wrong
    }
    NES_ERROR("Node: could not remove parent from  old node:" << oldOperator->toString());

    return false;
}

NodePtr OperatorNode::getChildWithOperatorId(uint64_t operatorId) {

    if (id == operatorId) {
        return shared_from_this();
    }
    for (auto& child : children) {
        auto found = child->as<OperatorNode>()->getChildWithOperatorId(operatorId);
        if (found) {
            return found;
        }
    }
    return nullptr;
}

void OperatorNode::addProperty(const std::string& key, const std::any value) { properties[key] = value; }

std::any OperatorNode::getProperty(const std::string& key) { return properties[key]; }

bool OperatorNode::hasProperty(const std::string& key) { return properties.find(key) != properties.end(); }

void OperatorNode::removeProperty(const std::string& key) { properties.erase(key); }

}// namespace NES
