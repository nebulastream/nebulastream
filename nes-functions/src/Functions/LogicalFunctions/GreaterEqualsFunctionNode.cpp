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
#include <Functions/LogicalFunctions/GreaterEqualsFunctionNode.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES
{

GreaterEqualsFunctionNode::GreaterEqualsFunctionNode() noexcept : LogicalBinaryFunctionNode("GreaterEquals")
{
}

GreaterEqualsFunctionNode::GreaterEqualsFunctionNode(GreaterEqualsFunctionNode* other) : LogicalBinaryFunctionNode(other)
{
}

FunctionNodePtr GreaterEqualsFunctionNode::create(const FunctionNodePtr& left, const FunctionNodePtr& right)
{
    auto greaterThen = std::make_shared<GreaterEqualsFunctionNode>();
    greaterThen->setChildren(left, right);
    return greaterThen;
}

bool GreaterEqualsFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<GreaterEqualsFunctionNode>())
    {
        auto other = rhs->as<GreaterEqualsFunctionNode>();
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

std::string GreaterEqualsFunctionNode::toString() const
{
    std::stringstream ss;
    ss << children[0]->toString() << ">=" << children[1]->toString();
    return ss.str();
}

FunctionNodePtr GreaterEqualsFunctionNode::deepCopy()
{
    return GreaterEqualsFunctionNode::create(children[0]->as<FunctionNode>()->deepCopy(), children[1]->as<FunctionNode>()->deepCopy());
}

bool GreaterEqualsFunctionNode::validate() const
{
    NES_NOT_IMPLEMENTED();
}

}