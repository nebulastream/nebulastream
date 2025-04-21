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
#include <Abstract/LogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/RoundLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>

namespace NES
{

RoundLogicalFunction::RoundLogicalFunction(LogicalFunction child) : stamp(child.getStamp().clone()), child(child) {};

RoundLogicalFunction::RoundLogicalFunction(const RoundLogicalFunction& other) : stamp(other.stamp->clone()), child(other.child)
{
}

bool RoundLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const RoundLogicalFunction*>(&rhs);
    if (other)
    {
        return child == other->getChildren()[0];
    }
    return false;
}

std::string RoundLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "ROUND(" << child << ")";
    return ss.str();
}

SerializableFunction RoundLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    auto* funcDesc = new SerializableFunction_UnaryFunction();
    auto* child_ = funcDesc->mutable_child();
    child_->CopyFrom(child.serialize());

    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

UnaryLogicalFunctionRegistryReturnType
UnaryLogicalFunctionGeneratedRegistrar::RegisterRoundUnaryLogicalFunction(UnaryLogicalFunctionRegistryArguments arguments)
{
    return RoundLogicalFunction(arguments.children[0]);
}

}
