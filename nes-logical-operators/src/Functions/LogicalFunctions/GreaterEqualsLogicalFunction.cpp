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
#include <string>
#include <Functions/LogicalFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

GreaterEqualsLogicalFunction::GreaterEqualsLogicalFunction(const GreaterEqualsLogicalFunction& other)
    : left(other.left), right(other.right), stamp(other.stamp->clone())
{
}

GreaterEqualsLogicalFunction::GreaterEqualsLogicalFunction(LogicalFunction left, LogicalFunction right)
    : left(left), right(right), stamp(DataTypeProvider::provideDataType(LogicalType::BOOLEAN)) {}

bool GreaterEqualsLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const GreaterEqualsLogicalFunction*>(&rhs);
    if (other)
    {
        return left == other->left && right == other->right;
    }
    return false;
}

std::string GreaterEqualsLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << left << ">=" << right;
    return ss.str();
}

const DataType& GreaterEqualsLogicalFunction::getStamp() const
{
    return *stamp;
};

LogicalFunction GreaterEqualsLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return *this;
};

LogicalFunction GreaterEqualsLogicalFunction::withInferredStamp(Schema schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return this->withChildren(newChildren);
};

std::vector<LogicalFunction> GreaterEqualsLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction GreaterEqualsLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string GreaterEqualsLogicalFunction::getType() const
{
    return std::string(NAME);
}

SerializableFunction GreaterEqualsLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    auto* funcDesc = new SerializableFunction_BinaryFunction();
    auto* leftChild = funcDesc->mutable_leftchild();
    leftChild->CopyFrom(left.serialize());
    auto* rightChild = funcDesc->mutable_rightchild();
    rightChild->CopyFrom(right.serialize());

    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

std::unique_ptr<BinaryLogicalFunctionRegistryReturnType>
BinaryLogicalFunctionGeneratedRegistrar::RegisterGreaterEqualsBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return GreaterEqualsLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
