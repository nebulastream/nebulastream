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

#include <memory>
#include <sstream>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunctions/NodeFunctionGreaterEquals.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

NodeFunctionGreaterEquals::NodeFunctionGreaterEquals() noexcept : NodeFunctionLogicalBinary("GreaterEquals")
{
}

NodeFunctionGreaterEquals::NodeFunctionGreaterEquals(NodeFunctionGreaterEquals* other) : NodeFunctionLogicalBinary(other)
{
}

std::shared_ptr<NodeFunction>
NodeFunctionGreaterEquals::create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right)
{
    auto greaterThen = std::make_shared<NodeFunctionGreaterEquals>();
    greaterThen->setChildren(left, right);
    return greaterThen;
}

bool NodeFunctionGreaterEquals::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionGreaterEquals>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionGreaterEquals>(rhs);
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

std::string NodeFunctionGreaterEquals::toString() const
{
    std::stringstream ss;
    ss << *children[0] << ">=" << *children[1];
    return ss.str();
}

std::shared_ptr<NodeFunction> NodeFunctionGreaterEquals::deepCopy()
{
    return NodeFunctionGreaterEquals::create(
        Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}
}
