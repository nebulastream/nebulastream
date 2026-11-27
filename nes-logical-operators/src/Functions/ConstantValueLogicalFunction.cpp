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
#include <expected>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/ScalarTypes.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Types/NESType.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <DataTypeRegistry.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{
namespace
{
NESValue makeNarrowestUnsignedNESValue(const uint64_t value)
{
    if (value <= std::numeric_limits<uint8_t>::max())
    {
        return UInt8Type::createValue(static_cast<uint8_t>(value));
    }
    if (value <= std::numeric_limits<uint16_t>::max())
    {
        return UInt16Type::createValue(static_cast<uint16_t>(value));
    }
    if (value <= std::numeric_limits<uint32_t>::max())
    {
        return UInt32Type::createValue(static_cast<uint32_t>(value));
    }
    return UInt64Type::createValue(value);
}

NESValue makeNarrowestSignedNESValue(const int64_t value)
{
    if (value >= std::numeric_limits<int8_t>::min() and value <= std::numeric_limits<int8_t>::max())
    {
        return Int8Type::createValue(static_cast<int8_t>(value));
    }
    if (value >= std::numeric_limits<int16_t>::min() and value <= std::numeric_limits<int16_t>::max())
    {
        return Int16Type::createValue(static_cast<int16_t>(value));
    }
    if (value >= std::numeric_limits<int32_t>::min() and value <= std::numeric_limits<int32_t>::max())
    {
        return Int32Type::createValue(static_cast<int32_t>(value));
    }
    return Int64Type::createValue(value);
}

NESValue inferScalarNESValueFromLiteralString(const std::string_view literal)
{
    if (literal.size() >= 2 and literal.front() == '\'' and literal.back() == '\'')
    {
        return VarSizedType::createValue(std::string{literal.substr(1, literal.size() - 2)});
    }
    if (literal == "TRUE" or literal == "true")
    {
        return BooleanType::createValue(true);
    }
    if (literal == "FALSE" or literal == "false")
    {
        return BooleanType::createValue(false);
    }
    try
    {
        if (!literal.empty() and literal.front() == '-')
        {
            const auto signedVal = from_chars_with_exception<int64_t>(literal);
            if (signedVal < 0)
            {
                return makeNarrowestSignedNESValue(signedVal);
            }
            return makeNarrowestUnsignedNESValue(static_cast<uint64_t>(signedVal));
        }
        return makeNarrowestUnsignedNESValue(from_chars_with_exception<uint64_t>(literal));
    }
    catch (const Exception&)
    {
        return Float64Type::createValue(from_chars_with_exception<double>(literal));
    }
}

NESValue wrapOptionalIfNullable(NESValue nonNullVal, const bool nullable)
{
    if (!nullable)
    {
        return nonNullVal;
    }
    auto optType = OptionalType::make(nonNullVal.getType());
    auto constructed = optType.construct(NESValueParameters::positional({std::move(nonNullVal)}));
    INVARIANT(constructed.has_value(), "Failed to wrap scalar NESValue in OptionalType: {}", constructed.error().what());
    return std::move(*constructed);
}

NESValue createNESValueFromLegacyString(const DataType dataType, const std::string_view str, const bool isNull)
{
    if (isNull)
    {
        PRECONDITION(dataType.nullable, "Typed NULL ConstantValueLogicalFunction requires a nullable DataType, got {}", dataType);
        const DataType innerDt{dataType.type, DataType::NULLABLE::NOT_NULLABLE};
        return OptionalType::createNullValue(innerDt.toNESType());
    }
    if (dataType.type == DataType::Type::UNDEFINED)
    {
        return wrapOptionalIfNullable(inferScalarNESValueFromLiteralString(str), dataType.nullable);
    }

    switch (dataType.type)
    {
        case DataType::Type::UINT8:
            return wrapOptionalIfNullable(UInt8Type::createValue(from_chars_with_exception<uint8_t>(str)), dataType.nullable);
        case DataType::Type::UINT16:
            return wrapOptionalIfNullable(UInt16Type::createValue(from_chars_with_exception<uint16_t>(str)), dataType.nullable);
        case DataType::Type::UINT32:
            return wrapOptionalIfNullable(UInt32Type::createValue(from_chars_with_exception<uint32_t>(str)), dataType.nullable);
        case DataType::Type::UINT64:
            return wrapOptionalIfNullable(UInt64Type::createValue(from_chars_with_exception<uint64_t>(str)), dataType.nullable);
        case DataType::Type::INT8:
            return wrapOptionalIfNullable(Int8Type::createValue(from_chars_with_exception<int8_t>(str)), dataType.nullable);
        case DataType::Type::INT16:
            return wrapOptionalIfNullable(Int16Type::createValue(from_chars_with_exception<int16_t>(str)), dataType.nullable);
        case DataType::Type::INT32:
            return wrapOptionalIfNullable(Int32Type::createValue(from_chars_with_exception<int32_t>(str)), dataType.nullable);
        case DataType::Type::INT64:
            return wrapOptionalIfNullable(Int64Type::createValue(from_chars_with_exception<int64_t>(str)), dataType.nullable);
        case DataType::Type::FLOAT32:
            return wrapOptionalIfNullable(Float32Type::createValue(from_chars_with_exception<float>(str)), dataType.nullable);
        case DataType::Type::FLOAT64:
            return wrapOptionalIfNullable(Float64Type::createValue(from_chars_with_exception<double>(str)), dataType.nullable);
        case DataType::Type::BOOLEAN:
            return wrapOptionalIfNullable(BooleanType::createValue(from_chars_with_exception<bool>(str)), dataType.nullable);
        case DataType::Type::CHAR:
            return wrapOptionalIfNullable(CharType::createValue(from_chars_with_exception<char>(str)), dataType.nullable);
        case DataType::Type::VARSIZED:
            return wrapOptionalIfNullable(VarSizedType::createValue(std::string{str}), dataType.nullable);
        case DataType::Type::UNDEFINED:
            std::unreachable();
    }
    std::unreachable();
}
}

ConstantValueLogicalFunction::ConstantValueLogicalFunction(NESValue value) : value(std::move(value))
{
    PRECONDITION(
        DataTypeRegistry::scalarDataTypeClass().validate(this->value.getType()),
        "ConstantValueLogicalFunction requires a NESValue whose type satisfies ScalarDatatype");
}

ConstantValueLogicalFunction::ConstantValueLogicalFunction(
    const DataType dataType, const std::string_view constantValueAsString, const bool isNull)
    : ConstantValueLogicalFunction(createNESValueFromLegacyString(dataType, constantValueAsString, isNull))
{
}

std::expected<ConstantValueLogicalFunction, Exception> ConstantValueLogicalFunction::tryCreate(NESValue value)
{
    if (!DataTypeRegistry::scalarDataTypeClass().validate(value.getType()))
    {
        return std::unexpected{TypeInferenceException("ConstantValueLogicalFunction requires a ScalarDatatype NESValue")};
    }
    if (value.tryGetAs<std::monostate>() != nullptr && value.getType().tryGetAs<OptionalType>() == nullptr)
    {
        return std::unexpected{InvalidQuerySyntax("Cannot construct non-nullable scalar constant from NULL")};
    }
    return ConstantValueLogicalFunction{std::move(value)};
}

ConstantValueLogicalFunction ConstantValueLogicalFunction::createNull(const DataType dataType)
{
    PRECONDITION(dataType.nullable, "Typed NULL ConstantValueLogicalFunction requires a nullable DataType, got {}", dataType);
    const DataType innerDt{dataType.type, DataType::NULLABLE::NOT_NULLABLE};
    return ConstantValueLogicalFunction{OptionalType::createNullValue(innerDt.toNESType())};
}

const NESValue& ConstantValueLogicalFunction::getValue() const noexcept
{
    return value;
}

DataType ConstantValueLogicalFunction::getDataType() const
{
    if (const auto* legacyTrait = value.getType().tryGetTypeTrait<LegacyDataTypeTrait>())
    {
        return legacyTrait->getDataType();
    }
    return DataTypeProvider::provideDataType(DataType::Type::UNDEFINED);
}

ConstantValueLogicalFunction ConstantValueLogicalFunction::withDataType(const DataType& targetDataType) const
{
    if (isNullValue())
    {
        return createNull(targetDataType);
    }
    const auto targetNESType = targetDataType.toNESType();
    if (auto constructed = targetNESType.construct(NESValueParameters::positional({value})); constructed.has_value())
    {
        return ConstantValueLogicalFunction{std::move(*constructed)};
    }
    return ConstantValueLogicalFunction{targetDataType, getConstantValue(), /*isNull=*/false};
}

std::vector<LogicalFunction> ConstantValueLogicalFunction::getChildren() const
{
    return {};
}

ConstantValueLogicalFunction ConstantValueLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    return *this;
}

std::string_view ConstantValueLogicalFunction::getType() const
{
    return NAME;
}

bool ConstantValueLogicalFunction::operator==(const ConstantValueLogicalFunction& rhs) const
{
    return value == rhs.value;
}

std::string ConstantValueLogicalFunction::explain(const ExplainVerbosity verbosity) const
{
    const auto formatted = getConstantValue();
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ConstantValueLogicalFunction({} : {})", formatted, getDataType());
    }
    return formatted;
}

std::string ConstantValueLogicalFunction::getConstantValue() const
{
    if (isNullValue())
    {
        return "NULL";
    }
    if (const auto* legacyTrait = value.getType().tryGetTypeTrait<LegacyDataTypeTrait>())
    {
        if (const auto formattedExp = legacyTrait->formatConstantValue(value); formattedExp.has_value() && formattedExp->has_value())
        {
            return **formattedExp;
        }
    }
    return "<scalar>";
}

bool ConstantValueLogicalFunction::isNullValue() const noexcept
{
    return value.tryGetAs<std::monostate>() != nullptr;
}

LogicalFunction ConstantValueLogicalFunction::withInferredDataType(const Schema<Field, Unordered>&) const
{
    return *this;
}

Reflected
Reflector<ConstantValueLogicalFunction>::operator()(const ConstantValueLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedConstantValueLogicalFunction{
        .value = function.getConstantValue(),
        .dataType = function.getDataType(),
        .isNull = function.isNullValue()});
}

ConstantValueLogicalFunction
Unreflector<ConstantValueLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [value, dataType, isNull] = context.unreflect<detail::ReflectedConstantValueLogicalFunction>(reflected);
    return ConstantValueLogicalFunction{dataType, value, isNull};
}

/// NOLINTBEGIN(performance-unnecessary-value-param)
LogicalFunctionRegistryReturnType ConstantValueLogicalFunction::createConstantValue(LogicalFunctionRegistryArguments)
{
    PRECONDITION(false, "Function is only build directly via parser or via reflection, not using the registry");
    std::unreachable();
}

/// NOLINTEND(performance-unnecessary-value-param)

}
