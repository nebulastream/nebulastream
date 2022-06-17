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

        std::vector<NodePtr> thisChildren = this->children;
        std::vector<NodePtr> thisParents = this->parents;

        for (auto& thisParent : thisParents) {

            bool isRightUpStreamOperator = false;
            bool isLeftUpStreamOperator = false;

            //If thisParent is a logical binary operator then maintain the left and the right operator ids
            if (thisParent->instanceOf<LogicalBinaryOperatorNode>()) {

                auto logicalBinaryOperator = thisParent->as<LogicalBinaryOperatorNode>();
                isRightUpStreamOperator = logicalBinaryOperator->isRightUpstreamOperatorId(id);
                isLeftUpStreamOperator = logicalBinaryOperator->isLeftUpstreamOperatorId(id);

                //Assertions to make sure the operator was in one of the two lists (xor)
                NES_ASSERT(isLeftUpStreamOperator != isRightUpStreamOperator,
                           "An Operator must be and can only be either the left or right upstream operator.");
            }

            //Iterate over children of this and add them to the thisParent of this
            for (auto& thisChild : thisChildren) {

                NES_TRACE("OperatorNode: Add Child of this operator as Child of this operator's parent");
                thisParent->addChild(thisChild);

                // Update the left and the right operator list
                if (isRightUpStreamOperator) {
                    thisParent->as_if<LogicalBinaryOperatorNode>()->addRightUpStreamOperatorId(
                        thisChild->as<LogicalOperatorNode>()->getId());
                } else if (isLeftUpStreamOperator) {
                    thisParent->as_if<LogicalBinaryOperatorNode>()->addLeftUpStreamOperatorId(
                        thisChild->as<LogicalOperatorNode>()->getId());
                }

                NES_TRACE("OperatorNode: remove this node as parent of its Child");
                //remove this as parent to its Child
                thisChild->removeParent(shared_from_this());
            }

            NES_TRACE("OperatorNode: remove this operator as upstream of its downstream operator");
            thisParent->removeChild(shared_from_this());

            //Update the left and the right operator list
            if (isRightUpStreamOperator) {
                thisParent->as_if<LogicalBinaryOperatorNode>()->removeRightUpstreamOperatorId(id);
            } else if (isLeftUpStreamOperator) {
                thisParent->as_if<LogicalBinaryOperatorNode>()->removeLeftUpstreamOperatorId(id);
            }
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

        bool isRightUpStreamOperator = false;
        bool isLeftUpStreamOperator = false;

        if (parent->instanceOf<LogicalBinaryOperatorNode>()) {

            auto logicalBinaryOperator = parent->as<LogicalBinaryOperatorNode>();
            isRightUpStreamOperator = logicalBinaryOperator->isRightUpstreamOperatorId(id);
            isLeftUpStreamOperator = logicalBinaryOperator->isLeftUpstreamOperatorId(id);

            //Assertions to make sure the operator was in one of the two lists (xor)
            NES_ASSERT(isLeftUpStreamOperator != isRightUpStreamOperator,
                       "An Operator must be and can only be either the left or right upstream operator.");
        }

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
                    parent->as_if<LogicalBinaryOperatorNode>()->addRightUpStreamOperatorId(newOperator->getId());
                } else if (isLeftUpStreamOperator) {
                    parent->as_if<LogicalBinaryOperatorNode>()->addLeftUpStreamOperatorId(newOperator->getId());
                }
            }
        }

        parent->removeChild(shared_from_this());
        //Remove id of this operator from the list
        if (isRightUpStreamOperator) {
            parent->as_if<LogicalBinaryOperatorNode>()->removeRightUpstreamOperatorId(id);
        } else if (isLeftUpStreamOperator) {
            parent->as_if<LogicalBinaryOperatorNode>()->removeLeftUpstreamOperatorId(id);
        }
    }

    if (!addParent(newOperator)) {
        NES_ERROR("Node: Unable to add input node as parent to this node.");
        return false;
    }
    return true;
}

bool OperatorNode::insertBetweenThisAndChildNodes(const OperatorNodePtr& newOperator) {

    if (this->instanceOf<LogicalBinaryOperatorNode>()) {
        NES_ERROR("OperatorNode: Can not insert a new operator as the child of a binary operator.");
        return false;
    }

    if (newOperator->instanceOf<LogicalBinaryOperatorNode>()) {
        NES_ERROR(
            "OperatorNode: Can not insert a binary operator between two operators. We do not have logic to define the left or "
            "right side of new binary operator to insert.");
        return false;
    }

    if (newOperator.get() == this) {
        NES_WARNING("OperatorNode:  Adding node to its self so will skip insertBetweenThisAndParentNodes operation.");
        return true;
    }

    if (find(children, newOperator)) {
        NES_WARNING("OperatorNode: the node is already part of its parents so ignore insertBetweenThisAndParentNodes operation.");
        return true;
    }

    NES_DEBUG("OperatorNode: Create temporary copy of this nodes parents.");
    std::vector<NodePtr> thisChildren = children;

    NES_DEBUG("OperatorNode: Remove all children of this node.");
    removeChildren();

    if (!addChild(newOperator)) {
        NES_ERROR("OperatorNode: Unable to add input node as parent to this node.");
        return false;
    }

    NES_DEBUG("OperatorNode: Add copy of this nodes parent as parent to the input node.");
    for (const NodePtr& thisChild : thisChildren) {
        if (!newOperator->addChild(thisChild)) {
            NES_ERROR("OperatorNode: Unable to add thisChild of this node as thisChild to input node.");
            return false;
        }
    }
    return true;
}

bool OperatorNode::replace(const OperatorNodePtr& newOperator) {
    return replace(newOperator, shared_from_this()->as<OperatorNode>());
}

bool OperatorNode::replace(const OperatorNodePtr& newOperator, const OperatorNodePtr& oldOperator) {

    // perform validations before replacement
    if (!newOperator || !oldOperator) {
        NES_ERROR("OperatorNode: Can't replace null operators");
        return false;
    } else if (oldOperator->isIdentical(newOperator)) {
        NES_WARNING("OperatorNode: the new and old operatorynode was the same so will skip replace operation.");
        return true;
    }

    bool isOldBinaryOperator = oldOperator->instanceOf<LogicalBinaryOperatorNode>();
    bool isNewBinaryOperator = newOperator->instanceOf<LogicalBinaryOperatorNode>();

    //Logic to replace operators
    if (isOldBinaryOperator && !isNewBinaryOperator) {
        NES_ERROR("OperatorNode: can not replace a binary operator with a non-binary operator");
        return false;
    } else if (!isOldBinaryOperator && isNewBinaryOperator) {
        NES_ERROR("OperatorNode: can not replace a non-binary operator with a binary operator");
        return false;
    } else if (isOldBinaryOperator && isNewBinaryOperator) {// if new and old operator are of type binary

        auto oldBinaryOperator = oldOperator->as<LogicalBinaryOperatorNode>();
        auto newBinaryOperator = newOperator->as<LogicalBinaryOperatorNode>();

        //Migrate Left upstream operators of old operator to new operator
        auto oldLeftOperatorIds = oldBinaryOperator->getLeftUpStreamOperatorIds();
        for (auto oldLeftOperatorId : oldLeftOperatorIds) {

            auto child = oldBinaryOperator->getChildWithOperatorId(oldLeftOperatorId);
            //Add the child to new operator
            newBinaryOperator->addChild(child);
            newBinaryOperator->addLeftUpStreamOperatorId(oldLeftOperatorId);

            //remove the child from old operator
            oldBinaryOperator->removeChild(child);
            oldBinaryOperator->removeLeftUpstreamOperatorId(oldLeftOperatorId);
        }

        //Migrate Right upstream operators of old operator to new operator
        auto oldRightOperatorIds = oldBinaryOperator->getRightUpStreamOperatorIds();
        for (auto oldRightOperatorId : oldRightOperatorIds) {

            auto child = oldBinaryOperator->getChildWithOperatorId(oldRightOperatorId);
            //Add the child to new operator
            newBinaryOperator->addChild(child);
            newBinaryOperator->addRightUpStreamOperatorId(oldRightOperatorId);

            //remove the child from old operator
            oldBinaryOperator->removeChild(child);
            oldBinaryOperator->removeRightUpstreamOperatorId(oldRightOperatorId);
        }

        //Migrate downstream operators of the old operator to new operator
        auto parentsOfOldOperator = oldBinaryOperator->getParents();
        for (auto& parentOfOldOperator : parentsOfOldOperator) {

            bool isRightUpStreamOperator = false;
            bool isLeftUpStreamOperator = false;

            //If thisParent is a logical binary operator then maintain the left and the right operator ids
            if (parentOfOldOperator->instanceOf<LogicalBinaryOperatorNode>()) {

                auto logicalBinaryOperator = parentOfOldOperator->as<LogicalBinaryOperatorNode>();
                isRightUpStreamOperator = logicalBinaryOperator->isRightUpstreamOperatorId(oldBinaryOperator->getId());
                isLeftUpStreamOperator = logicalBinaryOperator->isLeftUpstreamOperatorId(oldBinaryOperator->getId());

                //Assertions to make sure the operator was in one of the two lists (xor)
                NES_ASSERT(isLeftUpStreamOperator != isRightUpStreamOperator,
                           "An Operator must be and can only be either the left or right upstream operator.");
            }

            NES_TRACE("OperatorNode: remove this operator as upstream of its downstream operator");
            parentOfOldOperator->removeChild(oldOperator);
            parentOfOldOperator->addChild(newOperator);

            //Update the left and the right operator list
            if (isRightUpStreamOperator) {
                parentOfOldOperator->as_if<LogicalBinaryOperatorNode>()->removeRightUpstreamOperatorId(oldOperator->getId());
                parentOfOldOperator->as_if<LogicalBinaryOperatorNode>()->addRightUpStreamOperatorId(newOperator->getId());
            } else if (isLeftUpStreamOperator) {
                parentOfOldOperator->as_if<LogicalBinaryOperatorNode>()->removeLeftUpstreamOperatorId(oldOperator->getId());
                parentOfOldOperator->as_if<LogicalBinaryOperatorNode>()->addLeftUpStreamOperatorId(newOperator->getId());
            }
        }

        return true;
    } else {// if current and old operators are unary operators then perform replacement
        return oldOperator->insertBetweenThisAndChildNodes(newOperator) && oldOperator->removeAndJoinParentAndChildren();
    }
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
