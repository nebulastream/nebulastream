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
#include <Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

MulExpressionNode::MulExpressionNode(DataTypePtr stamp) : ArithmeticalBinaryExpressionNode(std::move(stamp)) {};

MulExpressionNode::MulExpressionNode(MulExpressionNode* other) : ArithmeticalBinaryExpressionNode(other)
{
}

ExpressionNodePtr MulExpressionNode::create(const ExpressionNodePtr& left, const ExpressionNodePtr& right)
{
    auto mulNode = std::make_shared<MulExpressionNode>(left->getStamp());
    mulNode->setChildren(left, right);
    return mulNode;
}

bool MulExpressionNode::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<MulExpressionNode>(rhs))
    {
        auto otherMulNode = NES::Util::as<MulExpressionNode>(rhs);
        return getLeft()->equal(otherMulNode->getLeft()) && getRight()->equal(otherMulNode->getRight());
    }
    return false;
}

std::string MulExpressionNode::toString() const
{
    std::stringstream ss;
    ss << children[0]->toString() << "*" << children[1]->toString();
    return ss.str();
}

ExpressionNodePtr MulExpressionNode::copy()
{
    return MulExpressionNode::create(Util::as<ExpressionNode>(children[0])->copy(), Util::as<ExpressionNode>(children[1])->copy());
}

} /// namespace NES
