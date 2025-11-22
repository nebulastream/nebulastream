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

#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

LessLogicalFunction::LessLogicalFunction(LogicalFunction left, LogicalFunction right)
    : left(std::move(left))
    , right(std::move(right))
    , dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN, this->left.getDataType().joinNullable(this->right.getDataType())))
{
}

bool LessLogicalFunction::operator==(const LessLogicalFunction& rhs) const
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

DataType LessLogicalFunction::getDataType() const
{
    return dataType;
};

LessLogicalFunction LessLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction LessLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    const bool isNullable = std::ranges::any_of(newChildren, [](const auto& child) { return child.getDataType().isNullableAsBool(); });
    auto newDataType = this->getDataType();
    newDataType.isNullable = isNullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    return withDataType(newDataType).withChildren(newChildren);
};

std::vector<LogicalFunction> LessLogicalFunction::getChildren() const
{
    return {left, right};
};

LessLogicalFunction LessLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
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
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("LessLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return LessLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
