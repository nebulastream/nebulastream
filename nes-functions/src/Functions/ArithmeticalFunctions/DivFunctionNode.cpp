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
#include <Functions/ArithmeticalFunctions/DivFunctionNode.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

DivFunctionNode::DivFunctionNode(DataTypePtr stamp) : ArithmeticalBinaryFunctionNode(std::move(stamp)) {};

DivFunctionNode::DivFunctionNode(DivFunctionNode* other) : ArithmeticalBinaryFunctionNode(other)
{
}

FunctionNodePtr DivFunctionNode::create(const FunctionNodePtr& left, const FunctionNodePtr& right)
{
    auto divNode = std::make_shared<DivFunctionNode>(left->getStamp());
    divNode->setChildren(left, right);
    return divNode;
}

bool DivFunctionNode::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<DivFunctionNode>(rhs))
    {
        auto otherDivNode = NES::Util::as<DivFunctionNode>(rhs);
        return getLeft()->equal(otherDivNode->getLeft()) && getRight()->equal(otherDivNode->getRight());
    }
    return false;
}

std::string DivFunctionNode::toString() const
{
    std::stringstream ss;
    ss << children[0]->toString() << "/" << children[1]->toString();
    return ss.str();
}

FunctionNodePtr DivFunctionNode::copy()
{
    return DivFunctionNode::create(Util::as<FunctionNode>(children[0])->copy(), Util::as<FunctionNode>(children[1])->copy());
}

}
