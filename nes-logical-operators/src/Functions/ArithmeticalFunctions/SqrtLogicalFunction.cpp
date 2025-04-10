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
#include <Functions/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <UnaryLogicalFunctionRegistry.hpp>

namespace NES
{

SqrtLogicalFunction::SqrtLogicalFunction(LogicalFunction child) : stamp(child.getStamp()), child(child)
{
};

SqrtLogicalFunction::SqrtLogicalFunction(const SqrtLogicalFunction& other) : stamp(other.stamp), child(other.child)
{
}

bool SqrtLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const SqrtLogicalFunction*>(&rhs);
    if(other)
    {
        return child == other->child;
    }
    return false;
}

std::string SqrtLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "SQRT(" << child << ")";
    return ss.str();
}

std::shared_ptr<DataType> SqrtLogicalFunction::getStamp() const
{
    return stamp;
};

LogicalFunction SqrtLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

LogicalFunction SqrtLogicalFunction::withInferredStamp(Schema schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> SqrtLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction SqrtLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string SqrtLogicalFunction::getType() const
{
    return std::string(NAME);
}

SerializableFunction SqrtLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

UnaryLogicalFunctionRegistryReturnType
UnaryLogicalFunctionGeneratedRegistrar::RegisterSqrtUnaryLogicalFunction(UnaryLogicalFunctionRegistryArguments arguments)
{
    return SqrtLogicalFunction(arguments.child);
}

}
