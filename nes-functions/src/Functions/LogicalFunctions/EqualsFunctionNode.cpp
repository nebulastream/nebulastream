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

namespace NES
{

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

FunctionNodePtr EqualsFunctionNode::copy()
{
    return EqualsFunctionNode::create(Util::as<FunctionNode>(children[0])->copy(), Util::as<FunctionNode>(children[1])->copy());
}

} /// namespace NES
