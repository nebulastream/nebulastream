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
#include <Functions/ArithmeticalFunctions/RoundLogicalFunction.hpp>
#include <Abstract/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <UnaryLogicalFunctionRegistry.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>

namespace NES
{

RoundLogicalFunction::RoundLogicalFunction(std::unique_ptr<LogicalFunction> child) : UnaryLogicalFunction(child->getStamp().clone(), std::move(child))
{
};

RoundLogicalFunction::RoundLogicalFunction(const RoundLogicalFunction& other) : UnaryLogicalFunction(other)
{
}

bool RoundLogicalFunction::operator==(const LogicalFunction& rhs) const
{
    auto other = dynamic_cast<const RoundLogicalFunction*>(&rhs);
    if (other)
    {
        return getChild() == other->getChild();
    }
    return false;
}

std::string RoundLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "ROUND(" << getChild() << ")";
    return ss.str();
}

std::unique_ptr<LogicalFunction> RoundLogicalFunction::clone() const
{
    return std::make_unique<RoundLogicalFunction>(getChild().clone());
}

SerializableFunction RoundLogicalFunction::serialize() const
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
UnaryLogicalFunctionGeneratedRegistrar::RegisterRoundUnaryLogicalFunction(UnaryLogicalFunctionRegistryArguments arguments)
{
    return std::make_unique<RoundLogicalFunction>(std::move(arguments.child));
}

}
