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
#include <ostream>
#include <utility>
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionSub.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

NodeFunctionSub::NodeFunctionSub(std::shared_ptr<DataType> stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Sub") {};

NodeFunctionSub::NodeFunctionSub(NodeFunctionSub* other) : NodeFunctionArithmeticalBinary(other)
{
}

std::shared_ptr<NodeFunction> NodeFunctionSub::create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right)
{
    auto subNode = std::make_shared<NodeFunctionSub>(left->getStamp());
    subNode->setChildren(left, right);
    return subNode;
}

bool NodeFunctionSub::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionSub>(rhs))
    {
        auto otherSubNode = NES::Util::as<NodeFunctionSub>(rhs);
        return getLeft()->equal(otherSubNode->getLeft()) && getRight()->equal(otherSubNode->getRight());
    }
    return false;
}

std::ostream& NodeFunctionSub::toDebugString(std::ostream& os) const
{
    return os << *children[0] << " - " << *children[1];
}

std::shared_ptr<NodeFunction> NodeFunctionSub::deepCopy()
{
    return NodeFunctionSub::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}

}
