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
#include <LogicalFunctions/Function.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <FunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <fmt/format.h>
#include <LogicalFunctions/LogicalFunctions/LessFunction.hpp>

namespace NES::Logical {

LessFunction::LessFunction(const LessFunction& other) : left(other.left), right(other.right), stamp(other.stamp)
{
}

LessFunction::LessFunction(Function left, Function right)
    : left(left), right(right), stamp(DataTypeProvider::provideDataType(LogicalType::BOOLEAN))
{
}

bool LessFunction::operator==(const FunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const LessFunction*>(&rhs))
    {
        return this->left == other->left && this->right == other->right;
    }
    return false;
}

std::string LessFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} < {}", left.explain(verbosity), right.explain(verbosity));
}

std::shared_ptr<DataType> LessFunction::getStamp() const
{
    return stamp;
};

Function LessFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

Function LessFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<Function> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<Function> LessFunction::getChildren() const
{
    return {left, right};
};

Function LessFunction::withChildren(const std::vector<Function>& children) const
{
    PRECONDITION(children.size() == 2, "LessFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view LessFunction::getType() const
{
    return NAME;
}

SerializableFunction LessFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

FunctionRegistryReturnType FunctionGeneratedRegistrar::RegisterLessFunction(FunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "LessFunction requires exactly two children, but got {}", arguments.children.size());
    return LessFunction(arguments.children[0], arguments.children[1]);
}

}
