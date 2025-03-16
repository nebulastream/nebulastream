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
#include <string>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Nodes/Node.hpp>
#include <fmt/format.h>

namespace NES
{
NodeFunctionConstantValue::NodeFunctionConstantValue(DataType type, std::string value)
    : NodeFunction(std::move(type), "ConstantValue"), constantValue(std::move(value))
{
}

NodeFunctionConstantValue::NodeFunctionConstantValue(const NodeFunctionConstantValue* other)
    : NodeFunction(other->getStamp(), "ConstantValue"), constantValue(other->constantValue)
{
}

bool NodeFunctionConstantValue::equal(const std::shared_ptr<Node>& rhs) const
{
    if (Util::instanceOf<NodeFunctionConstantValue>(rhs))
    {
        auto otherConstantValueNode = Util::as<NodeFunctionConstantValue>(rhs);
        return otherConstantValueNode->stamp == stamp && constantValue == otherConstantValueNode->constantValue;
    }
    return false;
}

std::string NodeFunctionConstantValue::toString() const
{
    return fmt::format("ConstantValue({}, {})", constantValue, stamp);
}

std::shared_ptr<NodeFunction> NodeFunctionConstantValue::create(DataType type, std::string value)
{
    return std::make_shared<NodeFunctionConstantValue>(NodeFunctionConstantValue(std::move(type), std::move(value)));
}

std::string NodeFunctionConstantValue::getConstantValue() const
{
    return constantValue;
}

void NodeFunctionConstantValue::inferStamp(const Schema&)
{
    /// the stamp of constant value functions is defined by the constant value type.
    /// thus it is already assigned correctly when the function node is created.
}

std::shared_ptr<NodeFunction> NodeFunctionConstantValue::deepCopy()
{
    return std::make_shared<NodeFunctionConstantValue>(*this);
}

bool NodeFunctionConstantValue::validateBeforeLowering() const
{
    return children.empty();
}

}
