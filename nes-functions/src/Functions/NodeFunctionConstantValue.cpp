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

#include <Functions/NodeFunctionConstantValue.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/ValueTypes/ValueType.hpp>

namespace NES
{
NodeFunctionConstantValue::NodeFunctionConstantValue(ValueTypePtr const& constantValue)
    : NodeFunction(constantValue->dataType, "ConstantValue"), constantValue(constantValue) {};

NodeFunctionConstantValue::NodeFunctionConstantValue(const NodeFunctionConstantValue* other)
    : NodeFunction(other->constantValue->dataType, "ConstantValue"), constantValue(other->constantValue)
{
}

bool NodeFunctionConstantValue::equal(NodePtr const& rhs) const
{
    if (Util::instanceOf<NodeFunctionConstantValue>(rhs))
    {
        auto otherConstantValueNode = Util::as<NodeFunctionConstantValue>(rhs);
        return otherConstantValueNode->constantValue->isEquals(constantValue);
    }
    return false;
}

std::string NodeFunctionConstantValue::toString() const
{
    return "ConstantValue(" + constantValue->toString() + ")";
}

NodeFunctionPtr NodeFunctionConstantValue::create(ValueTypePtr const& constantValue)
{
    return std::make_shared<NodeFunctionConstantValue>(NodeFunctionConstantValue(constantValue));
}

ValueTypePtr NodeFunctionConstantValue::getConstantValue() const
{
    return constantValue;
}

void NodeFunctionConstantValue::inferStamp(SchemaPtr)
{
    /// the stamp of constant value functions is defined by the constant value type.
    /// thus ut is already assigned correctly when the function node is created.
}

NodeFunctionPtr NodeFunctionConstantValue::deepCopy()
{
    return std::make_shared<NodeFunctionConstantValue>(*this);
}

bool NodeFunctionConstantValue::validateBeforeLowering() const
{
    return children.empty();
}

}
