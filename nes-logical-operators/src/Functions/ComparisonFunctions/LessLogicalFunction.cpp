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
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

LessLogicalFunction::LessLogicalFunction(const LessLogicalFunction& other) : left(other.left), right(other.right), dataType(other.dataType)
{
}

LessLogicalFunction::LessLogicalFunction(LogicalFunction left, LogicalFunction right)
    : left(std::move(std::move(left)))
    , right(std::move(std::move(right)))
    , dataType(DataTypeProvider::provideDataType(LogicalType::BOOLEAN))
{
}

bool LessLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const LessLogicalFunction*>(&rhs))
    {
        return this->left == other->left && this->right == other->right;
    }
    return false;
}

std::string LessLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{} < {}", left.explain(verbosity), right.explain(verbosity));
}

std::shared_ptr<DataType> LessLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction LessLogicalFunction::withDataType(std::shared_ptr<DataType> dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction LessLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> LessLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction LessLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "LessLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view LessLogicalFunction::getType() const
{
    return NAME;
}

SerializableFunction LessLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterLessLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.children.size() == 2, "LessLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    return LessLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
