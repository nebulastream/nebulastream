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

#include <Functions/ConstantValueFunctionNode.hpp>
#include <Common/ValueTypes/ValueType.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{
ConstantValueFunctionNode::ConstantValueFunctionNode(ValueTypePtr const& constantValue)
    : FunctionNode(constantValue->dataType, "ConstantValue"), constantValue(constantValue) {};

ConstantValueFunctionNode::ConstantValueFunctionNode(const ConstantValueFunctionNode* other)
    : FunctionNode(other->constantValue->dataType, "ConstantValue"), constantValue(other->constantValue)
{
}

bool ConstantValueFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<ConstantValueFunctionNode>())
    {
        auto otherConstantValueNode = rhs->as<ConstantValueFunctionNode>();
        return otherConstantValueNode->constantValue->isEquals(constantValue);
    }
    return false;
}

std::string ConstantValueFunctionNode::toString() const
{
    return "ConstantValue(" + constantValue->toString() + ")";
}

FunctionNodePtr ConstantValueFunctionNode::create(ValueTypePtr const& constantValue)
{
    return std::make_shared<ConstantValueFunctionNode>(ConstantValueFunctionNode(constantValue));
}

ValueTypePtr ConstantValueFunctionNode::getConstantValue() const
{
    return constantValue;
}

void ConstantValueFunctionNode::inferStamp(SchemaPtr)
{
    /// the stamp of constant value functions is defined by the constant value type.
    /// thus ut is already assigned correctly when the function node is created.
}

FunctionNodePtr ConstantValueFunctionNode::deepCopy()
{
    return std::make_shared<ConstantValueFunctionNode>(*this);
}

bool ConstantValueFunctionNode::validate() const
{
    NES_NOT_IMPLEMENTED();
}

}