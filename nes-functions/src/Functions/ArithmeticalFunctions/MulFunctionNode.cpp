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
#include <utility>
#include <Functions/ArithmeticalFunctions/MulFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
namespace NES
{
MulFunctionNode::MulFunctionNode(DataTypePtr stamp) : ArithmeticalBinaryFunctionNode(std::move(stamp), "Mul"){};

MulFunctionNode::MulFunctionNode(MulFunctionNode* other) : ArithmeticalBinaryFunctionNode(other)
{
}

FunctionNodePtr MulFunctionNode::create(const FunctionNodePtr& left, const FunctionNodePtr& right)
{
    auto mulNode = std::make_shared<MulFunctionNode>(left->getStamp());
    mulNode->setChildren(left, right);
    return mulNode;
}

bool MulFunctionNode::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<MulFunctionNode>(rhs))
    {
        auto otherMulNode = NES::Util::as<MulFunctionNode>(rhs);
        return getLeft()->equal(otherMulNode->getLeft()) && getRight()->equal(otherMulNode->getRight());
    }
    return false;
}

std::string MulFunctionNode::toString() const
{
    std::stringstream ss;
    ss << children[0]->toString() << "*" << children[1]->toString();
    return ss.str();
}

FunctionNodePtr MulFunctionNode::deepCopy()
{
    return MulFunctionNode::create(children[0]->as<FunctionNode>()->deepCopy(), children[1]->as<FunctionNode>()->deepCopy());
}

bool MulFunctionNode::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }
    return this->getChildren()[0]->as<FunctionNode>()->getStamp()->isNumeric()
        && this->getChildren()[1]->as<FunctionNode>()->getStamp()->isNumeric();
}
}
