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
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

NegateLogicalFunction::NegateLogicalFunction(LogicalFunction child)
    : stamp(DataTypeProvider::provideDataType(LogicalType::BOOLEAN)), child(child)
{
}

NegateLogicalFunction::NegateLogicalFunction(const NegateLogicalFunction& other) : stamp(other.stamp), child(other.child)
{
}

bool NegateLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const NegateLogicalFunction*>(&rhs);
    if (other)
    {
        return this->child == other->getChildren()[0];
    }
    return false;
}

std::string NegateLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "!" << child;
    return ss.str();
}

LogicalFunction NegateLogicalFunction::withInferredStamp(Schema schema) const
{
    auto newChild = child.withInferredStamp(schema);
    if (*newChild.getStamp().get() != Boolean())
    {
        throw CannotInferSchema(
            "Negate Function Node: the stamp of child must be boolean, but was: {}", child.getStamp().get()->toString());
    }
    return withChildren({newChild});
}

bool NegateLogicalFunction::validateBeforeLowering() const
{
    return dynamic_cast<const Boolean*>(child.getStamp().get());
}

std::shared_ptr<DataType> NegateLogicalFunction::getStamp() const
{
    return stamp;
};

LogicalFunction NegateLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

std::vector<LogicalFunction> NegateLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction NegateLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string NegateLogicalFunction::getType() const
{
    return std::string(NAME);
}

SerializableFunction NegateLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterNegateLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    return NegateLogicalFunction(arguments.children[0]);
}

}
