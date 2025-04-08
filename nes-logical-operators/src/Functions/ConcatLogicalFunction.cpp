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
#include <Functions/LogicalFunction.hpp>
#include <Functions/ConcatLogicalFunction.hpp>
#include <fmt/format.h>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <BinaryLogicalFunctionRegistry.hpp>

namespace NES
{

ConcatLogicalFunction::ConcatLogicalFunction(LogicalFunction left, LogicalFunction right) : stamp(left.getStamp().join(right.getStamp())), left(left), right(right)
{
}

ConcatLogicalFunction::ConcatLogicalFunction(const ConcatLogicalFunction& other) : stamp(other.stamp->clone()), left(other.left), right(other.right)
{
}

bool ConcatLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const ConcatLogicalFunction*>(&rhs))
    {
        return left == other->left and left == other->right;
    }
    return false;
}

std::string ConcatLogicalFunction::toString() const
{
    return fmt::format("Concat({}, {})", left, right);
}

const DataType& ConcatLogicalFunction::getStamp() const
{
    return *stamp;
};

LogicalFunction ConcatLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return *this;
};

LogicalFunction ConcatLogicalFunction::withInferredStamp(Schema schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> ConcatLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction ConcatLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string ConcatLogicalFunction::getType() const
{
    return std::string(NAME);
}

SerializableFunction ConcatLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

BinaryLogicalFunctionRegistryReturnType
BinaryLogicalFunctionGeneratedRegistrar::RegisterConcatBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return ConcatLogicalFunction(arguments.leftChild, arguments.rightChild);
}

}
