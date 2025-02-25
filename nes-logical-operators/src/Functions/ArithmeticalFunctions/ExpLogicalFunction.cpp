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

#include <Functions/ArithmeticalFunctions/ExpLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <UnaryLogicalFunctionRegistry.hpp>

namespace NES
{

ExpLogicalFunction::ExpLogicalFunction(std::unique_ptr<LogicalFunction> child) : UnaryLogicalFunction(child->getStamp().clone(), std::move(child))
{
};

ExpLogicalFunction::ExpLogicalFunction(const ExpLogicalFunction& other) : UnaryLogicalFunction(other)
{
}

bool ExpLogicalFunction::operator==(const LogicalFunction& rhs) const
{
    auto other = dynamic_cast<const ExpLogicalFunction*>(&rhs);
    if (other)
    {
        return getChild() == other->getChild();
    }
    return false;
}

std::string ExpLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "EXP(" << getChild() << ")";
    return ss.str();
}

std::unique_ptr<LogicalFunction> ExpLogicalFunction::clone() const
{
    return std::make_unique<ExpLogicalFunction>(getChild().clone());
}

SerializableFunction ExpLogicalFunction::serialize() const
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
UnaryLogicalFunctionGeneratedRegistrar::RegisterExpUnaryLogicalFunction(UnaryLogicalFunctionRegistryArguments arguments)
{
    return std::make_unique<ExpLogicalFunction>(std::move(arguments.child));
}
}
