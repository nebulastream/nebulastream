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

#include <algorithm>
#include <utility>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Operator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES
{
/**
 * @brief We initialize the input and output schemas with empty schemas.
 */
Operator::Operator(const OperatorId id) : id(id)
{
}

OperatorId Operator::getId() const
{
    return id;
}

void Operator::setId(const OperatorId id)
{
    Operator::id = id;
}

bool Operator::hasMultipleChildrenOrParents() const
{
    ///has multiple child operator
    bool hasMultipleChildren = (!getChildren().empty()) && getChildren().size() > 1;
    ///has multiple parent operator
    bool hasMultipleParent = (!getParents().empty()) && getParents().size() > 1;
    NES_DEBUG("Operator: has multiple children {} or has multiple parent {}", hasMultipleChildren, hasMultipleParent);
    return hasMultipleChildren || hasMultipleParent;
}

bool Operator::hasMultipleChildren() const
{
    return !getChildren().empty() && getChildren().size() > 1;
}

bool Operator::hasMultipleParents() const
{
    return !getParents().empty() && getParents().size() > 1;
}

OperatorPtr Operator::duplicate()
{
    NES_INFO("Operator: Create copy of the operator");
    const OperatorPtr copyOperator = copy();

    NES_DEBUG("Operator: copy all parents");
    for (const auto& parent : getParents())
    {
        if (!copyOperator->addParent(getDuplicateOfParent(NES::Util::as<Operator>(parent))))
        {
            NES_THROW_RUNTIME_ERROR("Operator: Unable to add parent to copy");
        }
    }

    NES_DEBUG("Operator: copy all children");
    for (const auto& child : getChildren())
    {
        if (!copyOperator->addChild(getDuplicateOfChild(NES::Util::as<Operator>(child)->duplicate())))
        {
            NES_THROW_RUNTIME_ERROR("Operator: Unable to add child to copy");
        }
    }
    return copyOperator;
}

OperatorPtr Operator::getDuplicateOfParent(const OperatorPtr& operatorNode)
{
    NES_DEBUG("Operator: create copy of the input operator");
    const OperatorPtr& copyOfOperator = operatorNode->copy();
    if (operatorNode->getParents().empty())
    {
        NES_TRACE("Operator: No ancestor of the input node. Returning the copy of the input operator");
        return copyOfOperator;
    }

    NES_TRACE("Operator: For all parents get copy of the ancestor and add as parent to the copy of the input operator");
    for (const auto& parent : operatorNode->getParents())
    {
        copyOfOperator->addParent(getDuplicateOfParent(NES::Util::as<Operator>(parent)));
    }
    NES_TRACE("Operator: return copy of the input operator");
    return copyOfOperator;
}

OperatorPtr Operator::getDuplicateOfChild(const OperatorPtr& operatorNode)
{
    NES_DEBUG("Operator: create copy of the input operator");
    OperatorPtr copyOfOperator = operatorNode->copy();
    if (operatorNode->getChildren().empty())
    {
        NES_TRACE("Operator: No children of the input node. Returning the copy of the input operator");
        return copyOfOperator;
    }

    NES_TRACE("Operator: For all children get copy of their children and add as child to the copy of the input operator");
    for (const auto& child : operatorNode->getChildren())
    {
        copyOfOperator->addChild(getDuplicateOfParent(NES::Util::as<Operator>(child)));
    }
    NES_TRACE("Operator: return copy of the input operator");
    return copyOfOperator;
}

bool Operator::addChild(const NodePtr newNode)
{
    if (!newNode)
    {
        NES_ERROR("Operator: Can't add null node");
        return false;
    }

    if (isIdentical(newNode))
    {
        NES_ERROR("Operator: can not add self as child to itself");
        return false;
    }

    std::vector<NodePtr> currentChildren = getChildren();
    auto found = std::find_if(
        currentChildren.begin(),
        currentChildren.end(),
        [&](const NodePtr& child) { return NES::Util::as<Operator>(child)->getId() == NES::Util::as<Operator>(newNode)->getId(); });

    if (found == currentChildren.end())
    {
        NES_TRACE("Operator: Adding node {} to the children.", *newNode);
        children.push_back(newNode);
        newNode->addParent(shared_from_this());
        return true;
    }
    NES_TRACE("Operator: the node is already part of its children so skip add child operation.");
    return false;
}

bool Operator::addParent(const NodePtr newNode)
{
    if (!newNode)
    {
        NES_ERROR("Operator: Can't add null node");
        return false;
    }

    if (isIdentical(newNode))
    {
        NES_ERROR("Operator: can not add self as parent to itself");
        return false;
    }

    std::vector<NodePtr> currentParents = getParents();
    auto found = std::find_if(
        currentParents.begin(),
        currentParents.end(),
        [&](const NodePtr& child) { return NES::Util::as<Operator>(child)->getId() == NES::Util::as<Operator>(newNode)->getId(); });

    if (found == currentParents.end())
    {
        NES_TRACE("Operator: Adding node {} to the Parents.", *newNode);
        parents.push_back(newNode);
        newNode->addChild(shared_from_this());
        return true;
    }
    NES_TRACE("Operator: the node is already part of its parent so skip add parent operation.");
    return false;
}

NodePtr Operator::getChildWithOperatorId(const OperatorId operatorId) const
{
    for (const auto& child : children)
    {
        /// If the child has a matching operator id then return it
        if (NES::Util::as<Operator>(child)->getId() == operatorId)
        {
            return child;
        }

        /// Look in for a matching operator in the grand children list
        auto found = NES::Util::as<Operator>(child)->getChildWithOperatorId(operatorId);
        if (found)
        {
            return found;
        }
    }
    return nullptr;
}

void Operator::addProperty(const std::string& key, const std::any value)
{
    properties[key] = value;
}

std::any Operator::getProperty(const std::string& key)
{
    return properties[key];
}

bool Operator::hasProperty(const std::string& key) const
{
    return properties.contains(key);
}

void Operator::removeProperty(const std::string& key)
{
    properties.erase(key);
}

bool Operator::containAsGrandChild(const NodePtr operatorNode)
{
    auto operatorIdToCheck = NES::Util::as<Operator>(operatorNode)->getId();
    /// populate all ancestors
    std::vector<NodePtr> ancestors{};
    for (auto& child : children)
    {
        std::vector<NodePtr> childAndGrandChildren = child->getAndFlattenAllChildren(false);
        ancestors.insert(ancestors.end(), childAndGrandChildren.begin(), childAndGrandChildren.end());
    }
    ///Check if an operator with the id exists as ancestor
    return std::any_of(
        ancestors.begin(),
        ancestors.end(),
        [operatorIdToCheck](const NodePtr& ancestor) { return NES::Util::as<Operator>(ancestor)->getId() == operatorIdToCheck; });
}

bool Operator::containAsGrandParent(const NodePtr operatorNode)
{
    auto operatorIdToCheck = NES::Util::as<Operator>(operatorNode)->getId();
    /// populate all ancestors
    std::vector<NodePtr> ancestors{};
    for (const auto& parent : parents)
    {
        std::vector<NodePtr> parentAndAncestors = parent->getAndFlattenAllAncestors();
        ancestors.insert(ancestors.end(), parentAndAncestors.begin(), parentAndAncestors.end());
    }
    ///Check if an operator with the id exists as ancestor
    return std::any_of(
        ancestors.begin(),
        ancestors.end(),
        [operatorIdToCheck](const NodePtr& ancestor) { return NES::Util::as<Operator>(ancestor)->getId() == operatorIdToCheck; });
}
void Operator::addAllProperties(const OperatorProperties& properties)
{
    for (auto& [key, value] : properties)
    {
        addProperty(key, value);
    }
}

OperatorId getNextOperatorId()
{
    static std::atomic_uint64_t id = INITIAL_OPERATOR_ID.getRawValue();
    return OperatorId(id++);
}

std::string Operator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "operatorId: " << id << "\n";
    out << "properties: ";
    for (const auto& item : properties)
    {
        if (item.first != properties.begin()->first)
        {
            out << ", ";
        }
        out << item.first;
    }
    out << std::endl;
    return out.str();
}

}
