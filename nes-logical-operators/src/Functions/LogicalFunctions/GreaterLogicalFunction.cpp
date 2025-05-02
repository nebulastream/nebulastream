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
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

GreaterLogicalFunction::GreaterLogicalFunction(const GreaterLogicalFunction& other)
    : stamp(other.stamp), left(other.left), right(other.right)
{
}

GreaterLogicalFunction::GreaterLogicalFunction(LogicalFunction left, LogicalFunction right)
    : stamp(DataTypeProvider::provideDataType(LogicalType::BOOLEAN)), left(left), right(right)
{
}

bool GreaterLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const GreaterLogicalFunction*>(&rhs);
    if (other)
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string GreaterLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    std::stringstream ss;
    ss << left.explain(verbosity) << " > " << right.explain(verbosity);
    return ss.str();
}

std::shared_ptr<DataType> GreaterLogicalFunction::getStamp() const
{
    return stamp;
};

LogicalFunction GreaterLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

LogicalFunction GreaterLogicalFunction::withInferredStamp(Schema schema) const
{
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
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterGreaterLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    return GreaterLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
