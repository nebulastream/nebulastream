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
#include <ostream>
#include <ranges>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/Operator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

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

std::shared_ptr<Operator> Operator::duplicate()
{
    NES_INFO("Operator: Create copy of the operator");
    const std::shared_ptr<Operator> copyOperator = copy();

    NES_DEBUG("Operator: copy all parents");
    for (const auto& parent : getParents())
    {
        const auto success = copyOperator->addParent(getDuplicateOfParent(NES::Util::as<Operator>(parent)));
        INVARIANT(success, "Unable to add parent ({}) to copy ({})", *NES::Util::as<Operator>(parent), *copyOperator);
    }

    NES_DEBUG("Operator: copy all children");
    for (const auto& child : getChildren())
    {
        const auto success = copyOperator->addChild(getDuplicateOfChild(NES::Util::as<Operator>(child)->duplicate()));
        INVARIANT(success, "Unable to add child ({}) to copy ({})", *NES::Util::as<Operator>(child), *copyOperator);
    }
    return copyOperator;
}

std::shared_ptr<Operator> Operator::getDuplicateOfParent(const std::shared_ptr<Operator>& operatorNode)
{
    NES_DEBUG("Operator: create copy of the input operator");
    const std::shared_ptr<Operator>& copyOfOperator = operatorNode->copy();
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

std::shared_ptr<Operator> Operator::getDuplicateOfChild(const std::shared_ptr<Operator>& operatorNode)
{
    NES_DEBUG("Operator: create copy of the input operator");
    std::shared_ptr<Operator> copyOfOperator = operatorNode->copy();
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

bool Operator::addChild(const std::shared_ptr<Node>& newNode)
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

    std::vector<std::shared_ptr<Node>> currentChildren = getChildren();
    const auto found = std::ranges::find_if(
        currentChildren,
        [&](const std::shared_ptr<Node>& child)
        { return NES::Util::as<Operator>(child)->getId() == NES::Util::as<Operator>(newNode)->getId(); });

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

bool Operator::addParent(const std::shared_ptr<Node>& newNode)
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

    std::vector<std::shared_ptr<Node>> currentParents = getParents();
    const auto found = std::ranges::find_if(
        currentParents,
        [&](const std::shared_ptr<Node>& child)
        { return NES::Util::as<Operator>(child)->getId() == NES::Util::as<Operator>(newNode)->getId(); });

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

std::shared_ptr<Node> Operator::getChildWithOperatorId(const OperatorId operatorId) const
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

bool Operator::containAsGrandChild(const std::shared_ptr<Node>& operatorNode)
{
    auto operatorIdToCheck = NES::Util::as<Operator>(operatorNode)->getId();
    /// populate all ancestors
    std::vector<std::shared_ptr<Node>> ancestors{};
    for (const auto& child : children)
    {
        std::vector<std::shared_ptr<Node>> childAndGrandChildren = child->getAndFlattenAllChildren(false);
        ancestors.insert(ancestors.end(), childAndGrandChildren.begin(), childAndGrandChildren.end());
    }
    ///Check if an operator with the id exists as ancestor
    return std::ranges::any_of(
        ancestors,
        [operatorIdToCheck](const std::shared_ptr<Node>& ancestor)
        { return NES::Util::as<Operator>(ancestor)->getId() == operatorIdToCheck; });
}

bool Operator::containAsGrandParent(const std::shared_ptr<Node>& operatorNode)
{
    auto operatorIdToCheck = NES::Util::as<Operator>(operatorNode)->getId();
    /// populate all ancestors
    std::vector<std::shared_ptr<Node>> ancestors{};
    for (const auto& parent : parents)
    {
        std::vector<std::shared_ptr<Node>> parentAndAncestors = parent->getAndFlattenAllAncestors();
        ancestors.insert(ancestors.end(), parentAndAncestors.begin(), parentAndAncestors.end());
    }
    ///Check if an operator with the id exists as ancestor
    return std::ranges::any_of(
        ancestors,
        [operatorIdToCheck](const std::shared_ptr<Node>& ancestor)
        { return NES::Util::as<Operator>(ancestor)->getId() == operatorIdToCheck; });
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

std::ostream& Operator::toDebugString(std::ostream& os) const
{
    return os << fmt::format("\nOperatorId: {}\nproperties: {}\n", id, fmt::join(std::ranges::views::keys(properties), ", "));
}

}
