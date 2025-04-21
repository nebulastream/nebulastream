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
#include <Functions/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>

namespace NES
{

RoundLogicalFunction::RoundLogicalFunction(LogicalFunction child) : stamp(child.getStamp()), child(child)  {};

RoundLogicalFunction::RoundLogicalFunction(const RoundLogicalFunction& other) :  stamp(other.stamp), child(other.child)
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


std::shared_ptr<DataType> RoundLogicalFunction::getStamp() const
{
    return stamp;
};

LogicalFunction RoundLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

LogicalFunction RoundLogicalFunction::withInferredStamp(Schema schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> RoundLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction RoundLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string RoundLogicalFunction::getType() const
{
    return std::string(NAME);
}

SerializableFunction RoundLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterRoundLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    return RoundLogicalFunction(arguments.children[0]);
}

}
