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

#include <Functions/FieldAccessLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include "Operators/LogicalOperator.hpp"

namespace NES
{
FieldAccessLogicalFunction::FieldAccessLogicalFunction(Field field) : field(std::move(field)) { };

bool FieldAccessLogicalFunction::operator==(const FieldAccessLogicalFunction& rhs) const
{
    return this->field == rhs.field;
}

bool operator!=(const FieldAccessLogicalFunction& lhs, const FieldAccessLogicalFunction& rhs)
{
    return !(lhs == rhs);
}

Field FieldAccessLogicalFunction::getField() const
{
    return field;
}

FieldAccessLogicalFunction FieldAccessLogicalFunction::withField(Field field) const
{
    auto copy = *this;
    copy.field = std::move(field);
    return copy;
}

std::string FieldAccessLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("FieldAccessLogicalFunction({})", field);
    }
    return fmt::format("{}", field);
}

LogicalFunction FieldAccessLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    const auto newField = schema.getFieldByName(field.getLastName());
    if (!newField)
    {
        throw CannotInferSchema("field {} is not part of the schema {}", field.getLastName(), schema);
    }

    auto copy = *this;
    copy.field = newField.value();
    return copy;
}

DataType FieldAccessLogicalFunction::getDataType() const
{
    return field.getDataType();
};

std::vector<LogicalFunction> FieldAccessLogicalFunction::getChildren() const
{
    return {};
};

FieldAccessLogicalFunction FieldAccessLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    return *this;
};

std::string_view FieldAccessLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<FieldAccessLogicalFunction>::operator()(const FieldAccessLogicalFunction& function) const
{
    return reflect(function.getField());
}

FieldAccessLogicalFunction
Unreflector<FieldAccessLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return FieldAccessLogicalFunction{context.unreflect<Field>(reflected)};
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterFieldAccessLogicalFunction(LogicalFunctionRegistryArguments)
{
    PRECONDITION(false, "Function is only build directly via parser or via reflection, not using the registry");
    std::unreachable();
}
}

std::size_t
std::hash<NES::FieldAccessLogicalFunction>::operator()(const NES::FieldAccessLogicalFunction& fieldAccessFunction) const noexcept
{
    return std::hash<NES::Field>()(fieldAccessFunction.getField());
}
