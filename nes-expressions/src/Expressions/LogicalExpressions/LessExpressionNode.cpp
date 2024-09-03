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

#include <Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
LessExpressionNode::LessExpressionNode(LessExpressionNode* other) : LogicalBinaryExpressionNode(other)
{
}

ExpressionNodePtr LessExpressionNode::create(const ExpressionNodePtr& left, const ExpressionNodePtr& right)
{
    auto lessThen = std::make_shared<LessExpressionNode>();
    lessThen->setChildren(left, right);
    return lessThen;
}

bool LessExpressionNode::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<LessExpressionNode>(rhs))
    {
        auto other = NES::Util::as<LessExpressionNode>(rhs);
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

std::string LessExpressionNode::toString() const
{
    std::stringstream ss;
    ss << children[0]->toString() << "<" << children[1]->toString();
    return ss.str();
}

ExpressionNodePtr LessExpressionNode::copy()
{
    return LessExpressionNode::create(Util::as<ExpressionNode>(children[0])->copy(), Util::as<ExpressionNode>(children[1])->copy());
}

} /// namespace NES
