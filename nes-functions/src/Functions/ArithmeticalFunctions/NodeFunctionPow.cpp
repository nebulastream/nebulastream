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
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionPow.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

NodeFunctionPow::NodeFunctionPow(DataType stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Pow") {};

NodeFunctionPow::NodeFunctionPow(NodeFunctionPow* other) : NodeFunctionArithmeticalBinary(other)
{
}

std::shared_ptr<NodeFunction> NodeFunctionPow::create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right)
{
    auto powNode = std::make_shared<NodeFunctionPow>(DataTypeProvider::provideDataType(DataType::Type::FLOAT32));
    powNode->setChildren(left, right);
    return powNode;
}

bool NodeFunctionPow::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionPow>(rhs))
    {
        auto otherAddNode = NES::Util::as<NodeFunctionPow>(rhs);
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

std::ostream& NodeFunctionPow::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 2, "Cannot print function without exactly two children.");
    return os << fmt::format("POWER({}, {})", *children.at(0), *children.at(1));
}

std::shared_ptr<NodeFunction> NodeFunctionPow::deepCopy()
{
    return NodeFunctionPow::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}
}
