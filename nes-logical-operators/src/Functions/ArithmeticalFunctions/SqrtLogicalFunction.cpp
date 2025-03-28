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
#include <Functions/ArithmeticalFunctions/SqrtLogicalFunction.hpp>
#include <Abstract/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <UnaryLogicalFunctionRegistry.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>

namespace NES
{

SqrtLogicalFunction::SqrtLogicalFunction(std::unique_ptr<LogicalFunction> child) : UnaryLogicalFunction(std::move(child))
{
    stamp = child->getStamp().clone();
};

SqrtLogicalFunction::SqrtLogicalFunction(const SqrtLogicalFunction& other) : UnaryLogicalFunction(other)
{
}

bool SqrtLogicalFunction::operator==(const LogicalFunction& rhs) const
{
    auto other = dynamic_cast<const SqrtLogicalFunction*>(&rhs);
    if(other)
    {
        return getChild() == other->getChild();
    }
    return false;
}

std::string SqrtLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "SQRT(" << getChild() << ")";
    return ss.str();
}

std::unique_ptr<LogicalFunction> SqrtLogicalFunction::clone() const
{
    return std::make_unique<SqrtLogicalFunction>(getChild().clone());
}

SerializableFunction SqrtLogicalFunction::serialize() const
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
UnaryLogicalFunctionGeneratedRegistrar::RegisterSqrtUnaryLogicalFunction(UnaryLogicalFunctionRegistryArguments arguments)
{
    return std::make_unique<SqrtLogicalFunction>(std::move(arguments.child));
}

}
