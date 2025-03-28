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

CeilLogicalFunction::CeilLogicalFunction(std::unique_ptr<LogicalFunction> child) : UnaryLogicalFunction( std::move(child))
{
    stamp = child->getStamp().clone();
};

CeilLogicalFunction::CeilLogicalFunction(const CeilLogicalFunction& other) : UnaryLogicalFunction(other)
{
}

bool CeilLogicalFunction::operator==(const LogicalFunction& rhs) const
{
    auto other = dynamic_cast<const CeilLogicalFunction*>(&rhs);
    if (other)
    {
        return getChild() == other->getChild();
    }
    return false;
}

std::string CeilLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "CEIL(" << getChild() << ")";
    return ss.str();
}

std::unique_ptr<LogicalFunction> CeilLogicalFunction::clone() const
{
    return std::make_unique<CeilLogicalFunction>(getChild().clone());
}

SerializableFunction CeilLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    auto* funcDesc = new SerializableFunction_UnaryFunction();
    auto* child = funcDesc->mutable_child();
    child->CopyFrom(getChild().serialize());

    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

UnaryLogicalFunctionRegistryReturnType
UnaryLogicalFunctionGeneratedRegistrar::RegisterCeilUnaryLogicalFunction(UnaryLogicalFunctionRegistryArguments arguments)
{
    return std::make_unique<CeilLogicalFunction>(std::move(arguments.child));
}

}
