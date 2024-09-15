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

#include <sstream>
#include <Functions/LogicalFunctions/EqualsFunctionNode.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES
{

EqualsFunctionNode::EqualsFunctionNode() noexcept : LogicalBinaryFunctionNode("Equals")
{
}

EqualsFunctionNode::EqualsFunctionNode(EqualsFunctionNode* other) : LogicalBinaryFunctionNode(other)
{
}

FunctionNodePtr EqualsFunctionNode::create(const FunctionNodePtr& left, const FunctionNodePtr& right)
{
    auto equals = std::make_shared<EqualsFunctionNode>();
    equals->setChildren(left, right);
    return equals;
}

bool EqualsFunctionNode::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<EqualsFunctionNode>(rhs))
    {
        auto other = NES::Util::as<EqualsFunctionNode>(rhs);
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

std::string EqualsFunctionNode::toString() const
{
    std::stringstream ss;
    ss << children[0]->toString() << "==" << children[1]->toString();
    return ss.str();
}

FunctionNodePtr EqualsFunctionNode::deepCopy()
{
    return EqualsFunctionNode::create(children[0]->as<FunctionNode>()->deepCopy(), children[1]->as<FunctionNode>()->deepCopy());
}

bool EqualsFunctionNode::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }

    const auto childLeft = children[0]->as<FunctionNode>();
    const auto childRight = children[1]->as<FunctionNode>();

    /// If one of the children has a stamp of type text, the other child must also have a stamp of type text
    if (childLeft->getStamp()->isText() != childRight->getStamp()->isText())
    {
        return false;
    }


    /// If one of the children has a stamp of type array, the other child must also have a stamp of type array
    if (childLeft->getStamp()->isArray() || childRight->getStamp()->isArray() || childLeft->getStamp()->isCharArray() || childRight->getStamp()->isCharArray())
    {
        NES_ERROR("We do not support array and char arrays as data types for now!");
        return false;
    }

    return true;
}

}
