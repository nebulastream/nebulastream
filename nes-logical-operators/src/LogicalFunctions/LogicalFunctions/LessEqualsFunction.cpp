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

#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <FunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <fmt/format.h>
#include <LogicalFunctions/LogicalFunctions/LessEqualsFunction.hpp>

namespace NES::Logical {

LessEqualsFunction::LessEqualsFunction(const LessEqualsFunction& other)
    : left(other.left), right(other.right), stamp(other.stamp)
{
}

LessEqualsFunction::LessEqualsFunction(Function left, Function right)
    : left(left), right(right), stamp(DataTypeProvider::provideDataType(LogicalType::BOOLEAN))
{
}


bool LessEqualsFunction::operator==(const FunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const LessEqualsFunction*>(&rhs))
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = left == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string LessEqualsFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} <= {}", left.explain(verbosity), right.explain(verbosity));
}

std::shared_ptr<DataType> LessEqualsFunction::getStamp() const
{
    return stamp;
};

Function LessEqualsFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

Function LessEqualsFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<Function> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return this->withChildren(newChildren);
};

std::vector<Function> LessEqualsFunction::getChildren() const
{
    return {left, right};
};

Function LessEqualsFunction::withChildren(const std::vector<Function>& children) const
{
    PRECONDITION(children.size() == 2, "LessEqualsFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view LessEqualsFunction::getType() const
{
    return NAME;
}


SerializableFunction LessEqualsFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

FunctionRegistryReturnType
FunctionGeneratedRegistrar::RegisterLessEqualsFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "LessEqualsFunction requires exactly two children, but got {}", arguments.children.size());
    return LessEqualsFunction(arguments.children[0], arguments.children[1]);
}

}
