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
#include <LogicalFunctions/Function.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <FunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <fmt/format.h>
#include <LogicalFunctions/LogicalFunctions/EqualsFunction.hpp>

namespace NES::Logical {

EqualsFunction::EqualsFunction(Function left, Function right)
    : left(left), right(right), stamp(DataTypeProvider::provideDataType(LogicalType::BOOLEAN))
{
}

EqualsFunction::EqualsFunction(const EqualsFunction& other) : left(other.left), right(other.right), stamp(other.stamp)
{
}

bool EqualsFunction::operator==(const FunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const EqualsFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::shared_ptr<DataType> EqualsFunction::getStamp() const
{
    return stamp;
};

Function EqualsFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

Function EqualsFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<Function> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<Function> EqualsFunction::getChildren() const
{
    return {left, right};
};

Function EqualsFunction::withChildren(const std::vector<Function>& children) const
{
    PRECONDITION(children.size() == 2, "EqualsFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view EqualsFunction::getType() const
{
    return NAME;
}

std::string EqualsFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} = {}", left.explain(verbosity), right.explain(verbosity));
}

bool EqualsFunction::validateBeforeLowering() const
{
    return dynamic_cast<const VariableSizedDataType*>(left.getStamp().get())
        && dynamic_cast<const VariableSizedDataType*>(right.getStamp().get());
}

SerializableFunction EqualsFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());

    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

FunctionRegistryReturnType
FunctionGeneratedRegistrar::RegisterEqualsFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "EqualsFunction requires exactly two children, but got {}", arguments.children.size());
    return EqualsFunction(arguments.children[0], arguments.children[1]);
}

}
