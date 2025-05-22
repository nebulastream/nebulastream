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
#include <utility>
#include <Functions/LogicalFunctions/NodeFunctionLogical.hpp>
#include <Functions/LogicalFunctions/NodeFunctionLogicalBinary.hpp>
#include <Functions/NodeFunctionBinary.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Char.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

namespace NES
{
NodeFunctionLogicalBinary::NodeFunctionLogicalBinary(std::string name)
    : NodeFunctionBinary(DataTypeProvider::provideDataType(LogicalType::BOOLEAN), std::move(name)), LogicalNodeFunction()
{
}

NodeFunctionLogicalBinary::NodeFunctionLogicalBinary(NodeFunctionLogicalBinary* other) : NodeFunctionBinary(other), LogicalNodeFunction()
{
}

bool NodeFunctionLogicalBinary::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionLogicalBinary>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionLogicalBinary>(rhs);
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

bool NodeFunctionLogicalBinary::validateBeforeLowering() const
{
    if (children.size() != 2)
    {
        return false;
    }

    const auto childLeft = Util::as<NodeFunction>(children[0]);
    const auto childRight = Util::as<NodeFunction>(children[1]);

    /// If one of the children has a stamp of type text, we do not support comparison for text or arrays at the moment
    if (NES::Util::instanceOf<VariableSizedDataType>(childLeft->getStamp())
        || NES::Util::instanceOf<VariableSizedDataType>(childRight->getStamp()))
    {
        return false;
    }

    return true;
}

}
