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

#include <Functions/LogicalFunctions/LessEqualsLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

LessEqualsLogicalFunction::LessEqualsLogicalFunction(const LessEqualsLogicalFunction& other) : left(other.left), right(other.right), stamp(other.stamp->clone())
{
}

LessEqualsLogicalFunction::LessEqualsLogicalFunction(LogicalFunction left, LogicalFunction right) : left(left), right(right), stamp(DataTypeProvider::provideDataType(LogicalType::BOOLEAN))
{
}


bool LessEqualsLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const LessEqualsLogicalFunction*>(&rhs))
    {
        return left == other->left && right == other->right;
    }
    return false;
}

std::string LessEqualsLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << left << "<=" << right;
    return ss.str();
}


const DataType& LessEqualsLogicalFunction::getStamp() const
{
    return *stamp;
};

LogicalFunction LessEqualsLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return *this;
};

LogicalFunction LessEqualsLogicalFunction::withInferredStamp(Schema schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return this->withChildren(newChildren);
};

std::vector<LogicalFunction> LessEqualsLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction LessEqualsLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string LessEqualsLogicalFunction::getType() const
{
    return std::string(NAME);
}


SerializableFunction LessEqualsLogicalFunction::serialize() const
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
BinaryLogicalFunctionGeneratedRegistrar::RegisterLessEqualsBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return LessEqualsLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
