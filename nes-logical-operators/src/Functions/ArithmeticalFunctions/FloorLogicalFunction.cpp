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

#include <Functions/ArithmeticalFunctions/FloorLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <UnaryLogicalFunctionRegistry.hpp>

namespace NES
{

FloorLogicalFunction::FloorLogicalFunction(LogicalFunction child) : stamp(child.getStamp().clone()), child(child)
{
};

FloorLogicalFunction::FloorLogicalFunction(const FloorLogicalFunction& other) : stamp(other.getStamp().clone()), child(other.child)
{
}

const DataType& FloorLogicalFunction::getStamp() const
{
    return *stamp;
};

LogicalFunction FloorLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return *this;
};

LogicalFunction FloorLogicalFunction::withInferredStamp(Schema schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return this->withChildren(newChildren);
};

std::vector<LogicalFunction> FloorLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction FloorLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string FloorLogicalFunction::getType() const
{
    return std::string(NAME);
}

bool FloorLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const FloorLogicalFunction*>(&rhs);
    if (other)
    {
        return child == other->child;
    }
    return false;
}

std::string FloorLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "FLOOR(" << child << ")";
    return ss.str();
}

SerializableFunction FloorLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

UnaryLogicalFunctionRegistryReturnType
UnaryLogicalFunctionGeneratedRegistrar::RegisterFloorUnaryLogicalFunction(UnaryLogicalFunctionRegistryArguments arguments)
{
    return FloorLogicalFunction(arguments.child);
}

}
