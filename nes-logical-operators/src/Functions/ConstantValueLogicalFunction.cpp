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

#include <Functions/ConstantValueLogicalFunction.hpp>

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{
namespace
{
DataType inferUnsignedIntegerLiteralDataType(const uint64_t value)
{
    if (value <= std::numeric_limits<uint8_t>::max())
    {
        return DataTypeProvider::provideDataType(DataType::Type::UINT8);
    }
    if (value <= std::numeric_limits<uint16_t>::max())
    {
        return DataTypeProvider::provideDataType(DataType::Type::UINT16);
    }
    if (value <= std::numeric_limits<uint32_t>::max())
    {
        return DataTypeProvider::provideDataType(DataType::Type::UINT32);
    }
    return DataTypeProvider::provideDataType(DataType::Type::UINT64);
}

DataType inferSignedIntegerLiteralDataType(const int64_t value)
{
    if (value >= std::numeric_limits<int8_t>::min() and value <= std::numeric_limits<int8_t>::max())
    {
        return DataTypeProvider::provideDataType(DataType::Type::INT8);
    }
    if (value >= std::numeric_limits<int16_t>::min() and value <= std::numeric_limits<int16_t>::max())
    {
        return DataTypeProvider::provideDataType(DataType::Type::INT16);
    }
    if (value >= std::numeric_limits<int32_t>::min() and value <= std::numeric_limits<int32_t>::max())
    {
        return DataTypeProvider::provideDataType(DataType::Type::INT32);
    }
    return DataTypeProvider::provideDataType(DataType::Type::INT64);
}

DataType inferIntegerLiteralDataType(const std::string_view literal)
{
    if (!literal.empty() and literal.front() == '-')
    {
        const auto value = from_chars_with_exception<int64_t>(literal);
        if (value < 0)
        {
            return inferSignedIntegerLiteralDataType(value);
        }
        return inferUnsignedIntegerLiteralDataType(static_cast<uint64_t>(value));
    }
    return inferUnsignedIntegerLiteralDataType(from_chars_with_exception<uint64_t>(literal));
}

DataType inferConstantValueDataType(const std::string_view literal)
{
    if (literal.size() >= 2 and literal.front() == '\'' and literal.back() == '\'')
    {
        return DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    }
    if (literal == "TRUE" or literal == "FALSE" or literal == "true" or literal == "false")
    {
        return DataTypeProvider::provideDataType(DataType::Type::BOOLEAN);
    }
    try
    {
        return inferIntegerLiteralDataType(literal);
    }
    catch (const Exception&)
    {
        from_chars_with_exception<double>(literal);
        return DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
    }
}
}

ConstantValueLogicalFunction::ConstantValueLogicalFunction(DataType dataType, std::string constantValueAsString)
    : constantValue(std::move(constantValueAsString)), dataType(std::move(dataType))
{
}

DataType ConstantValueLogicalFunction::getDataType() const
{
    return dataType;
};

ConstantValueLogicalFunction ConstantValueLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

std::vector<LogicalFunction> ConstantValueLogicalFunction::getChildren() const
{
    return {};
};

ConstantValueLogicalFunction ConstantValueLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    return *this;
};

std::string_view ConstantValueLogicalFunction::getType() const
{
    return NAME;
}

bool ConstantValueLogicalFunction::operator==(const ConstantValueLogicalFunction& rhs) const
{
    return constantValue == rhs.constantValue;
}

std::string ConstantValueLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ConstantValueLogicalFunction({} : {})", constantValue, dataType);
    }
    return constantValue;
}

std::string ConstantValueLogicalFunction::getConstantValue() const
{
    return constantValue;
}

LogicalFunction ConstantValueLogicalFunction::withInferredDataType(const Schema<Field, Unordered>&) const
{
    if (dataType.type != DataType::Type::UNDEFINED)
    {
        return *this;
    }
    auto inferredDataType = inferConstantValueDataType(constantValue);
    if (inferredDataType.isType(DataType::Type::VARSIZED))
    {
        return ConstantValueLogicalFunction(std::move(inferredDataType), constantValue.substr(1, constantValue.size() - 2));
    }
    return ConstantValueLogicalFunction(std::move(inferredDataType), constantValue);
}

Reflected
Reflector<ConstantValueLogicalFunction>::operator()(const ConstantValueLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedConstantValueLogicalFunction{.value = function.getConstantValue(), .dataType = function.getDataType()});
}

ConstantValueLogicalFunction
Unreflector<ConstantValueLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [value, dataType] = context.unreflect<detail::ReflectedConstantValueLogicalFunction>(reflected);
    return ConstantValueLogicalFunction{dataType, std::move(value)};
}

/// NOLINTBEGIN(performance-unnecessary-value-param)
LogicalFunctionRegistryReturnType ConstantValueLogicalFunction::createConstantValue(LogicalFunctionRegistryArguments)
{
    PRECONDITION(false, "Function is only build directly via parser or via reflection, not using the registry");
    std::unreachable();
}

/// NOLINTEND(performance-unnecessary-value-param)

}
