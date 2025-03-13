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

#include <Functions/LogicalFunctions/NegateLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <UnaryLogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

NegateLogicalFunction::NegateLogicalFunction(std::unique_ptr<LogicalFunction> child)
    : UnaryLogicalFunction(DataTypeProvider::provideDataType(LogicalType::BOOLEAN), std::move(child))
{
}

NegateLogicalFunction::NegateLogicalFunction(const NegateLogicalFunction& other) : UnaryLogicalFunction(other)
{
}

bool NegateLogicalFunction::operator==(const LogicalFunction& rhs) const
{
    auto other = dynamic_cast<const NegateLogicalFunction*>(&rhs);
    if (other)
    {
        return this->getChild() == other->getChild();
    }
    return false;
}

std::string NegateLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "!" << getChild();
    return ss.str();
}

void NegateLogicalFunction::inferStamp(const Schema& schema)
{
    /// delegate stamp inference of children
    LogicalFunction::inferStamp(schema);
    /// check if children stamp is correct
    if (!getChild().isPredicate())
    {
        throw CannotInferSchema(
            fmt::format("Negate Function Node: the stamp of child must be boolean, but was: {}", getChild().getStamp().toString()));
    }
}
std::unique_ptr<LogicalFunction> NegateLogicalFunction::clone() const
{
    return std::make_unique<NegateLogicalFunction>(getChild().clone());
}

bool NegateLogicalFunction::validateBeforeLowering() const
{
    return dynamic_cast<Boolean*>(&getChild().getStamp());
}

SerializableFunction NegateLogicalFunction::serialize() const
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

std::unique_ptr<UnaryLogicalFunctionRegistryReturnType>
UnaryLogicalFunctionGeneratedRegistrar::RegisterNegateUnaryLogicalFunction(UnaryLogicalFunctionRegistryArguments arguments)
{
    return std::make_unique<NegateLogicalFunction>(std::move(arguments.child));
}

}
