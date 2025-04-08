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

#include <Functions/LogicalFunctions/GreaterLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <BinaryLogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

GreaterLogicalFunction::GreaterLogicalFunction(const GreaterLogicalFunction& other) : stamp(other.stamp->clone()), left(other.left), right(other.right)
{
}

GreaterLogicalFunction::GreaterLogicalFunction(LogicalFunction left, LogicalFunction right) : stamp(DataTypeProvider::provideDataType(LogicalType::BOOLEAN)), left(left), right(right)
{
}

bool GreaterLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const GreaterLogicalFunction*>(&rhs);
    if (other)
    {
        return left == other->left && right == other->right;
    }
    return false;
}

std::string GreaterLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << left << ">" << right;
    return ss.str();
}


const DataType& GreaterLogicalFunction::getStamp() const
{
    return *stamp;
};

LogicalFunction GreaterLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return *this;
};

LogicalFunction GreaterLogicalFunction::withInferredStamp(Schema schema) const {
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return this->withChildren(newChildren);
}

std::vector<LogicalFunction> GreaterLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction GreaterLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string GreaterLogicalFunction::getType() const
{
    return std::string(NAME);
}

SerializableFunction GreaterLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

BinaryLogicalFunctionRegistryReturnType
BinaryLogicalFunctionGeneratedRegistrar::RegisterGreaterBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return GreaterLogicalFunction(arguments.leftChild, arguments.rightChild);
}

}
