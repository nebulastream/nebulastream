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
#include <sstream>
#include <Functions/LogicalFunctions/EqualsLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

EqualsLogicalFunction::EqualsLogicalFunction(LogicalFunction left, LogicalFunction right)
    : left(left), right(right), stamp(DataTypeProvider::provideDataType(LogicalType::BOOLEAN))
{
}

EqualsLogicalFunction::EqualsLogicalFunction(const EqualsLogicalFunction& other) : left(other.left), right(other.right), stamp(other.stamp->clone())
{
}

bool EqualsLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const EqualsLogicalFunction*>(&rhs);
    if (other)
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

const DataType& EqualsLogicalFunction::getStamp() const
{
    return *stamp;
};

LogicalFunction EqualsLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

LogicalFunction EqualsLogicalFunction::withInferredStamp(Schema schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> EqualsLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction EqualsLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string EqualsLogicalFunction::getType() const
{
    return std::string(NAME);
}

std::string EqualsLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << left << "==" << right;
    return ss.str();
}

bool EqualsLogicalFunction::validateBeforeLowering() const
{
    return dynamic_cast<const VariableSizedDataType*>(&left.getStamp())
        && dynamic_cast<const VariableSizedDataType*>(&right.getStamp());
}

SerializableFunction EqualsLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());

    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterEqualsLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    return EqualsLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
