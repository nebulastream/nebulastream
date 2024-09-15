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

#include <Functions/LogicalFunctions/OrFunctionNode.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
OrFunctionNode::OrFunctionNode() : LogicalBinaryFunctionNode("Or")
{
}

OrFunctionNode::OrFunctionNode(OrFunctionNode* other) : LogicalBinaryFunctionNode(other)
{
}

FunctionNodePtr OrFunctionNode::create(FunctionNodePtr const& left, FunctionNodePtr const& right)
{
    auto orNode = std::make_shared<OrFunctionNode>();
    orNode->setChildren(left, right);
    return orNode;
}

bool OrFunctionNode::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<OrFunctionNode>(rhs))
    {
        auto otherAndNode = NES::Util::as<OrFunctionNode>(rhs);
        return getLeft()->equal(otherAndNode->getLeft()) && getRight()->equal(otherAndNode->getRight());
    }
    return false;
}

std::string OrFunctionNode::toString() const
{
    std::stringstream ss;
    ss << children[0]->toString() << "||" << children[1]->toString();
    return ss.str();
}

void OrFunctionNode::inferStamp(SchemaPtr schema)
{
    /// delegate stamp inference of children
    FunctionNode::inferStamp(schema);
    /// check if children stamp is correct
    if (!getLeft()->isPredicate())
    {
        NES_THROW_RUNTIME_ERROR(
            "OR Function Node: the stamp of left child must be boolean, but was: " + getLeft()->getStamp()->toString());
    }
    if (!getRight()->isPredicate())
    {
        NES_THROW_RUNTIME_ERROR(
            "OR Function Node: the stamp of left child must be boolean, but was: " + getRight()->getStamp()->toString());
    }
}
FunctionNodePtr OrFunctionNode::deepCopy()
{
    return OrFunctionNode::create(children[0]->as<FunctionNode>()->deepCopy(), children[1]->as<FunctionNode>()->deepCopy());
}

bool OrFunctionNode::validateBeforeLowering() const
{
    return children.size() == 2;
}

}
