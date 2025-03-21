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

#include <Functions/ArithmeticalFunctions/CeilLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <UnaryLogicalFunctionRegistry.hpp>

namespace NES
{

CeilLogicalFunction::CeilLogicalFunction(LogicalFunction child) : stamp(child.getStamp().clone()), child(child)
{
};

CeilLogicalFunction::CeilLogicalFunction(const CeilLogicalFunction& other) : stamp(other.stamp->clone()), child(other.child)
{
}

bool CeilLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const CeilLogicalFunction*>(&rhs);
    if (other)
    {
        return child == other->child;
    }
    return false;
}

std::string CeilLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "CEIL(" << child << ")";
    return ss.str();
}

SerializableFunction CeilLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    auto* funcDesc = new SerializableFunction_UnaryFunction();
    auto* child_ = funcDesc->mutable_child();
    child_->CopyFrom(child.serialize());

    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

UnaryLogicalFunctionRegistryReturnType
UnaryLogicalFunctionGeneratedRegistrar::RegisterCeilUnaryLogicalFunction(UnaryLogicalFunctionRegistryArguments arguments)
{
    return CeilLogicalFunction(arguments.child);
}

}
