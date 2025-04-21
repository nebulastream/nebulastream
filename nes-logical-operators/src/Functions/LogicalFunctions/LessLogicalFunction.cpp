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
#include <Functions/LogicalFunctions/LessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

LessLogicalFunction::LessLogicalFunction(const LessLogicalFunction& other) : left(other.left), right(other.right), stamp(other.stamp)
{
}

LessLogicalFunction::LessLogicalFunction(LogicalFunction left, LogicalFunction right) : left(left), right(right), stamp(DataTypeProvider::provideDataType(LogicalType::BOOLEAN))
{
}

bool LessLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const LessLogicalFunction*>(&rhs);
    if (other)
    {
        return this->left == other->left && this->right == other->right;
    }
    return false;
}

std::string LessLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << left << "<" << right;
    return ss.str();
}

std::shared_ptr<DataType> LessLogicalFunction::getStamp() const
{
    return stamp;
};

LogicalFunction LessLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

LogicalFunction LessLogicalFunction::withInferredStamp(Schema schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> LessLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction LessLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string LessLogicalFunction::getType() const
{
    return std::string(NAME);
}

SerializableFunction LessLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterLessLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    return LessLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
