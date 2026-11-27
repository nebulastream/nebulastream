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

#include <DataTypes/ScalarTypes.hpp>

#include <concepts>
#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include <DataTypes/DataType.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Types/NESType.hpp>
#include <Util/Any.hpp>
#include <fmt/format.h>
#include <DataTypeRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
std::expected<NESValue, Exception> requireSinglePositionalArg(const NESValueParameters& params, const std::string_view typeName)
{
    if (params.isNamed())
    {
        return std::unexpected{InvalidQuerySyntax("{} constructor expects a positional argument, got named argument", typeName)};
    }
    if (params.size() != 1)
    {
        return std::unexpected{InvalidQuerySyntax("{} constructor expects exactly 1 argument, but got {}", typeName, params.size())};
    }
    auto arg = params.at(0).value();
    if (arg.tryGetAs<std::monostate>() != nullptr)
    {
        return std::unexpected{
            InvalidQuerySyntax("Cannot construct non-nullable scalar type {} from NULL; use OPTIONAL({})", typeName, typeName)};
    }
    return arg;
}

template <std::integral TargetInt>
std::expected<TargetInt, Exception> constructIntegerFromParams(const NESValueParameters& params, const std::string_view typeName)
{
    const auto argExp = requireSinglePositionalArg(params, typeName);
    if (!argExp.has_value())
    {
        return std::unexpected{argExp.error()};
    }
    const auto& arg = *argExp;

    const auto checkAndCast = [typeName]<std::integral SourceInt>(const SourceInt val) -> std::expected<TargetInt, Exception>
    {
        if (!std::in_range<TargetInt>(val))
        {
            return std::unexpected{InvalidQuerySyntax("Constant value {} is out of range for {}", val, typeName)};
        }
        return static_cast<TargetInt>(val);
    };

    if (const auto* v = arg.tryGetAs<int8_t>())
    {
        return checkAndCast(*v);
    }
    if (const auto* v = arg.tryGetAs<int16_t>())
    {
        return checkAndCast(*v);
    }
    if (const auto* v = arg.tryGetAs<int32_t>())
    {
        return checkAndCast(*v);
    }
    if (const auto* v = arg.tryGetAs<int64_t>())
    {
        return checkAndCast(*v);
    }
    if (const auto* v = arg.tryGetAs<uint8_t>())
    {
        return checkAndCast(*v);
    }
    if (const auto* v = arg.tryGetAs<uint16_t>())
    {
        return checkAndCast(*v);
    }
    if (const auto* v = arg.tryGetAs<uint32_t>())
    {
        return checkAndCast(*v);
    }
    if (const auto* v = arg.tryGetAs<uint64_t>())
    {
        return checkAndCast(*v);
    }
    return std::unexpected{InvalidQuerySyntax("Expected integer literal argument for {} constructor", typeName)};
}

template <std::floating_point TargetFloat>
std::expected<TargetFloat, Exception> constructFloatFromParams(const NESValueParameters& params, const std::string_view typeName)
{
    const auto argExp = requireSinglePositionalArg(params, typeName);
    if (!argExp.has_value())
    {
        return std::unexpected{argExp.error()};
    }
    const auto& arg = *argExp;
    const auto asDouble = tryExtractFloatingPoint(arg);
    if (!asDouble.has_value())
    {
        return std::unexpected{InvalidQuerySyntax("Expected numeric literal argument for {} constructor", typeName)};
    }
    return static_cast<TargetFloat>(*asDouble);
}

template <typename ValueT>
std::string formatTypedScalar(const ValueT& val)
{
    if constexpr (std::same_as<ValueT, bool>)
    {
        return val ? "true" : "false";
    }
    else if constexpr (std::same_as<ValueT, char>)
    {
        return std::string{1, val};
    }
    else if constexpr (std::same_as<ValueT, std::string>)
    {
        return val;
    }
    else if constexpr (std::same_as<ValueT, int8_t> || std::same_as<ValueT, uint8_t>)
    {
        return std::to_string(static_cast<int>(val));
    }
    else if constexpr (std::integral<ValueT>)
    {
        return std::to_string(val);
    }
    else if constexpr (std::floating_point<ValueT>)
    {
        return fmt::format("{}", val);
    }
    else
    {
        return "NULL";
    }
}

template <typename ScalarTypeStruct, DataType::Type PhysicalTag>
struct TypedLegacyDataTypeTrait
{
    [[nodiscard]] static DataType getDataType()
    {
        return DataType{PhysicalTag, DataType::NULLABLE::NOT_NULLABLE};
    }

    [[nodiscard]] static std::expected<std::optional<std::string>, Exception> formatConstantValue(const NESValue& value)
    {
        using ValueT = typename ScalarTypeStruct::ValueType;
        if constexpr (std::same_as<ValueT, std::monostate>)
        {
            return std::unexpected{TypeInferenceException("UNDEFINED has no constant literal representation")};
        }
        else
        {
            if (const auto* typedPtr = value.tryGetAs<ValueT>())
            {
                return std::optional<std::string>{formatTypedScalar(*typedPtr)};
            }
            return std::unexpected{TypeInferenceException("NESValue payload does not match {}", ScalarTypeStruct::NAME)};
        }
    }
};

struct OptionalLegacyDataTypeTrait
{
    DataType nullableDataType;
    LegacyDataTypeTrait innerLegacyTrait;

    [[nodiscard]] DataType getDataType() const { return nullableDataType; }

    [[nodiscard]] std::expected<std::optional<std::string>, Exception> formatConstantValue(const NESValue& value) const
    {
        if (value.tryGetAs<std::monostate>() != nullptr)
        {
            return std::optional<std::string>{std::nullopt};
        }
        return innerLegacyTrait.formatConstantValue(value);
    }
};

template <typename ConcreteScalarType>
struct NullaryScalarFunctor
{
    bool operator==(const NullaryScalarFunctor&) const = default;

    [[nodiscard]] static ExpectedNESTypeArguments getExpectedArguments() { return ExpectedNESTypeArguments{}; }

    [[nodiscard]] static std::expected<NESType, Exception> instantiate(const NESTypeParameters& params)
    {
        if (!params.empty())
        {
            return std::unexpected{TypeInferenceException(
                "Scalar data type functor '{}' expects 0 type parameters (use OPTIONAL(...) for nullability), but got {}",
                ConcreteScalarType::NAME,
                params.size())};
        }
        return ConcreteScalarType::makeNESType();
    }
};

/// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
const NESTypeParameter INNER_TYPE{
    Identifier::parse("INNER_TYPE"),
    "Scalar type wrapped with nullability",
    &DataTypeRegistry::scalarDataTypeClass};

struct OptionalFunctor
{
    bool operator==(const OptionalFunctor&) const = default;

    [[nodiscard]] static ExpectedNESTypeArguments getExpectedArguments()
    {
        return createExpectedNESTypeArguments(INNER_TYPE);
    }

    [[nodiscard]] static std::expected<NESType, Exception> instantiate(const InstantiatedNESTypeParameters& parameters)
    {
        return OptionalType::make(parameters.get(INNER_TYPE));
    }
};
}

/// BooleanType
std::expected<bool, Exception> BooleanType::construct(const NESValueParameters& params) const
{
    const auto argExp = requireSinglePositionalArg(params, NAME);
    if (!argExp.has_value())
    {
        return std::unexpected{argExp.error()};
    }
    if (const auto* b = argExp->tryGetAs<bool>())
    {
        return *b;
    }
    return std::unexpected{InvalidQuerySyntax("Expected boolean literal argument for BOOLEAN constructor")};
}

NESType BooleanType::makeNESType()
{
    static const NESType instance = NESType{BooleanType{}}
                                        .withTypeTrait(ScalarTypeTrait{})
                                        .withTypeTrait(LegacyDataTypeTrait{TypedLegacyDataTypeTrait<BooleanType, DataType::Type::BOOLEAN>{}});
    return instance;
}

NESValue BooleanType::createValue(const bool value)
{
    return NESValue::create(makeNESType(), value);
}

NESFunctor BooleanType::functor()
{
    return NESFunctor{NullaryScalarFunctor<BooleanType>{}};
}

/// CharType
std::expected<char, Exception> CharType::construct(const NESValueParameters& params) const
{
    const auto argExp = requireSinglePositionalArg(params, NAME);
    if (!argExp.has_value())
    {
        return std::unexpected{argExp.error()};
    }
    if (const auto* c = argExp->tryGetAs<char>())
    {
        return *c;
    }
    if (const auto* s = argExp->tryGetAs<std::string>(); s != nullptr && s->size() == 1)
    {
        return (*s)[0];
    }
    return std::unexpected{InvalidQuerySyntax("Expected single-character argument for CHAR constructor")};
}

NESType CharType::makeNESType()
{
    static const NESType instance = NESType{CharType{}}
                                        .withTypeTrait(ScalarTypeTrait{})
                                        .withTypeTrait(LegacyDataTypeTrait{TypedLegacyDataTypeTrait<CharType, DataType::Type::CHAR>{}});
    return instance;
}

NESValue CharType::createValue(const char value)
{
    return NESValue::create(makeNESType(), value);
}

NESFunctor CharType::functor()
{
    return NESFunctor{NullaryScalarFunctor<CharType>{}};
}

/// Signed integer types
#define NES_DEFINE_SIGNED_INT_SCALAR_TYPE(StructName, CppType, EnumTag) \
    std::expected<CppType, Exception> StructName::construct(const NESValueParameters& params) const \
    { \
        return constructIntegerFromParams<CppType>(params, NAME); \
    } \
    NESType StructName::makeNESType() \
    { \
        static const NESType instance = NESType{StructName{}} \
                                            .withTypeTrait(ScalarTypeTrait{}) \
                                            .withTypeTrait(LegacyDataTypeTrait{TypedLegacyDataTypeTrait<StructName, DataType::Type::EnumTag>{}}) \
                                            .withTypeTrait(NumericTypeTrait{}) \
                                            .withTypeTrait(IntegerTypeTrait{.isSigned = true}); \
        return instance; \
    } \
    NESValue StructName::createValue(const CppType value) \
    { \
        return NESValue::create(makeNESType(), value); \
    } \
    NESFunctor StructName::functor() \
    { \
        return NESFunctor{NullaryScalarFunctor<StructName>{}}; \
    }

NES_DEFINE_SIGNED_INT_SCALAR_TYPE(Int8Type, int8_t, INT8)
NES_DEFINE_SIGNED_INT_SCALAR_TYPE(Int16Type, int16_t, INT16)
NES_DEFINE_SIGNED_INT_SCALAR_TYPE(Int32Type, int32_t, INT32)
NES_DEFINE_SIGNED_INT_SCALAR_TYPE(Int64Type, int64_t, INT64)

#undef NES_DEFINE_SIGNED_INT_SCALAR_TYPE

/// Unsigned integer types
#define NES_DEFINE_UNSIGNED_INT_SCALAR_TYPE(StructName, CppType, EnumTag) \
    std::expected<CppType, Exception> StructName::construct(const NESValueParameters& params) const \
    { \
        return constructIntegerFromParams<CppType>(params, NAME); \
    } \
    NESType StructName::makeNESType() \
    { \
        static const NESType instance = NESType{StructName{}} \
                                            .withTypeTrait(ScalarTypeTrait{}) \
                                            .withTypeTrait(LegacyDataTypeTrait{TypedLegacyDataTypeTrait<StructName, DataType::Type::EnumTag>{}}) \
                                            .withTypeTrait(NumericTypeTrait{}) \
                                            .withTypeTrait(IntegerTypeTrait{.isSigned = false}); \
        return instance; \
    } \
    NESValue StructName::createValue(const CppType value) \
    { \
        return NESValue::create(makeNESType(), value); \
    } \
    NESFunctor StructName::functor() \
    { \
        return NESFunctor{NullaryScalarFunctor<StructName>{}}; \
    }

NES_DEFINE_UNSIGNED_INT_SCALAR_TYPE(UInt8Type, uint8_t, UINT8)
NES_DEFINE_UNSIGNED_INT_SCALAR_TYPE(UInt16Type, uint16_t, UINT16)
NES_DEFINE_UNSIGNED_INT_SCALAR_TYPE(UInt32Type, uint32_t, UINT32)
NES_DEFINE_UNSIGNED_INT_SCALAR_TYPE(UInt64Type, uint64_t, UINT64)

#undef NES_DEFINE_UNSIGNED_INT_SCALAR_TYPE

/// Floating-point types
std::expected<float, Exception> Float32Type::construct(const NESValueParameters& params) const
{
    return constructFloatFromParams<float>(params, NAME);
}

NESType Float32Type::makeNESType()
{
    static const NESType instance = NESType{Float32Type{}}
                                        .withTypeTrait(ScalarTypeTrait{})
                                        .withTypeTrait(LegacyDataTypeTrait{TypedLegacyDataTypeTrait<Float32Type, DataType::Type::FLOAT32>{}})
                                        .withTypeTrait(NumericTypeTrait{})
                                        .withTypeTrait(FloatTypeTrait{});
    return instance;
}

NESValue Float32Type::createValue(const float value)
{
    return NESValue::create(makeNESType(), value);
}

NESFunctor Float32Type::functor()
{
    return NESFunctor{NullaryScalarFunctor<Float32Type>{}};
}

std::expected<double, Exception> Float64Type::construct(const NESValueParameters& params) const
{
    return constructFloatFromParams<double>(params, NAME);
}

NESType Float64Type::makeNESType()
{
    static const NESType instance = NESType{Float64Type{}}
                                        .withTypeTrait(ScalarTypeTrait{})
                                        .withTypeTrait(LegacyDataTypeTrait{TypedLegacyDataTypeTrait<Float64Type, DataType::Type::FLOAT64>{}})
                                        .withTypeTrait(NumericTypeTrait{})
                                        .withTypeTrait(FloatTypeTrait{});
    return instance;
}

NESValue Float64Type::createValue(const double value)
{
    return NESValue::create(makeNESType(), value);
}

NESFunctor Float64Type::functor()
{
    return NESFunctor{NullaryScalarFunctor<Float64Type>{}};
}

/// VarSizedType
std::expected<std::string, Exception> VarSizedType::construct(const NESValueParameters& params) const
{
    const auto argExp = requireSinglePositionalArg(params, NAME);
    if (!argExp.has_value())
    {
        return std::unexpected{argExp.error()};
    }
    if (const auto* s = argExp->tryGetAs<std::string>())
    {
        return *s;
    }
    return std::unexpected{InvalidQuerySyntax("Expected string literal argument for VARSIZED constructor")};
}

NESType VarSizedType::makeNESType()
{
    static const NESType instance = NESType{VarSizedType{}}
                                        .withTypeTrait(ScalarTypeTrait{})
                                        .withTypeTrait(LegacyDataTypeTrait{TypedLegacyDataTypeTrait<VarSizedType, DataType::Type::VARSIZED>{}});
    return instance;
}

NESValue VarSizedType::createValue(std::string value)
{
    return NESValue::create(makeNESType(), std::move(value));
}

NESFunctor VarSizedType::functor()
{
    return NESFunctor{NullaryScalarFunctor<VarSizedType>{}};
}

/// UndefinedType
NESType UndefinedType::makeNESType()
{
    static const NESType instance = NESType{UndefinedType{}}
                                        .withTypeTrait(ScalarTypeTrait{})
                                        .withTypeTrait(LegacyDataTypeTrait{TypedLegacyDataTypeTrait<UndefinedType, DataType::Type::UNDEFINED>{}});
    return instance;
}

NESFunctor UndefinedType::functor()
{
    return NESFunctor{NullaryScalarFunctor<UndefinedType>{}};
}

/// OptionalType
std::expected<ExplicitAny, Exception> OptionalType::construct(const NESValueParameters& params) const
{
    if (!params.isNamed() && params.size() == 1)
    {
        const auto arg = params.at(0).value();
        if (arg.tryGetAs<std::monostate>() != nullptr)
        {
            return ExplicitAny::create(std::monostate{});
        }
    }
    return innerType.construct(params).transform([](const NESValue& val) { return val.getInternalRepresentation(); });
}

bool OptionalType::equalsValue(const ExplicitAny& lhs, const ExplicitAny& rhs) const
{
    const bool lhsNull = lhs.tryGetAs<std::monostate>() != nullptr;
    const bool rhsNull = rhs.tryGetAs<std::monostate>() != nullptr;
    if (lhsNull || rhsNull)
    {
        return lhsNull && rhsNull;
    }
    return innerType.equalsValue(lhs, rhs);
}

NESType OptionalType::make(NESType innerType)
{
    const bool isSingleLevel = innerType.tryGetAs<OptionalType>() == nullptr;
    std::optional<LegacyDataTypeTrait> optionalLegacyTrait;
    if (isSingleLevel)
    {
        if (const auto* innerLegacyTrait = innerType.tryGetTypeTrait<LegacyDataTypeTrait>())
        {
            const auto innerDt = innerLegacyTrait->getDataType();
            optionalLegacyTrait.emplace(OptionalLegacyDataTypeTrait{
                .nullableDataType = DataType{innerDt.type, DataType::NULLABLE::IS_NULLABLE},
                .innerLegacyTrait = *innerLegacyTrait});
        }
    }

    const bool hasNumeric = isSingleLevel && innerType.hasTypeTrait<NumericTypeTrait>();
    const auto* intTrait = isSingleLevel ? innerType.tryGetTypeTrait<IntegerTypeTrait>() : nullptr;
    const bool hasFloat = isSingleLevel && innerType.hasTypeTrait<FloatTypeTrait>();
    const std::optional<IntegerTypeTrait> copiedIntTrait = intTrait != nullptr ? std::optional{*intTrait} : std::nullopt;

    auto result = NESType{OptionalType{.innerType = std::move(innerType)}}.withTypeTrait(ScalarTypeTrait{});
    if (optionalLegacyTrait.has_value())
    {
        result = result.withTypeTrait(std::move(*optionalLegacyTrait));
    }
    if (hasNumeric)
    {
        result = result.withTypeTrait(NumericTypeTrait{});
    }
    if (copiedIntTrait.has_value())
    {
        result = result.withTypeTrait(*copiedIntTrait);
    }
    if (hasFloat)
    {
        result = result.withTypeTrait(FloatTypeTrait{});
    }
    return result;
}

NESValue OptionalType::createNullValue(NESType innerType)
{
    return NESValue::create(make(std::move(innerType)), std::monostate{});
}

NESFunctor OptionalType::functor()
{
    return NESFunctor{OptionalFunctor{}};
}

bool isOptionalNESType(const NESType& type) noexcept
{
    return type.tryGetAs<OptionalType>() != nullptr;
}

const NESType& unwrapOptionalNESType(const NESType& type) noexcept
{
    if (const auto* opt = type.tryGetAs<OptionalType>())
    {
        return opt->getInnerType();
    }
    return type;
}

NESType wrapOptionalNESTypeIf(NESType innerType, const bool nullable)
{
    if (!nullable || isOptionalNESType(innerType))
    {
        return innerType;
    }
    return OptionalType::make(std::move(innerType));
}

/// Typed extraction helpers
std::expected<int64_t, Exception> tryExtractSignedInteger(const NESValue& value)
{
    if (const auto* v = value.tryGetAs<int8_t>())
    {
        return static_cast<int64_t>(*v);
    }
    if (const auto* v = value.tryGetAs<int16_t>())
    {
        return static_cast<int64_t>(*v);
    }
    if (const auto* v = value.tryGetAs<int32_t>())
    {
        return static_cast<int64_t>(*v);
    }
    if (const auto* v = value.tryGetAs<int64_t>())
    {
        return *v;
    }
    if (const auto* v = value.tryGetAs<uint8_t>())
    {
        return static_cast<int64_t>(*v);
    }
    if (const auto* v = value.tryGetAs<uint16_t>())
    {
        return static_cast<int64_t>(*v);
    }
    if (const auto* v = value.tryGetAs<uint32_t>())
    {
        return static_cast<int64_t>(*v);
    }
    if (const auto* v = value.tryGetAs<uint64_t>())
    {
        if (!std::in_range<int64_t>(*v))
        {
            return std::unexpected{InvalidQuerySyntax("Integer literal {} exceeds the supported signed integer range", *v)};
        }
        return static_cast<int64_t>(*v);
    }
    return std::unexpected{InvalidQuerySyntax("Expected integer NESValue")};
}

std::expected<uint64_t, Exception> tryExtractUnsignedInteger(const NESValue& value)
{
    const auto checkSigned = []<std::signed_integral S>(const S v) -> std::expected<uint64_t, Exception>
    {
        if (v < 0)
        {
            return std::unexpected{InvalidQuerySyntax("Negative integer literal {} cannot be converted to unsigned integer", v)};
        }
        return static_cast<uint64_t>(v);
    };
    if (const auto* v = value.tryGetAs<uint8_t>())
    {
        return static_cast<uint64_t>(*v);
    }
    if (const auto* v = value.tryGetAs<uint16_t>())
    {
        return static_cast<uint64_t>(*v);
    }
    if (const auto* v = value.tryGetAs<uint32_t>())
    {
        return static_cast<uint64_t>(*v);
    }
    if (const auto* v = value.tryGetAs<uint64_t>())
    {
        return *v;
    }
    if (const auto* v = value.tryGetAs<int8_t>())
    {
        return checkSigned(*v);
    }
    if (const auto* v = value.tryGetAs<int16_t>())
    {
        return checkSigned(*v);
    }
    if (const auto* v = value.tryGetAs<int32_t>())
    {
        return checkSigned(*v);
    }
    if (const auto* v = value.tryGetAs<int64_t>())
    {
        return checkSigned(*v);
    }
    return std::unexpected{InvalidQuerySyntax("Expected unsigned integer NESValue")};
}

std::expected<double, Exception> tryExtractFloatingPoint(const NESValue& value)
{
    if (const auto* f = value.tryGetAs<float>())
    {
        return static_cast<double>(*f);
    }
    if (const auto* d = value.tryGetAs<double>())
    {
        return *d;
    }
    if (const auto signedInt = tryExtractSignedInteger(value); signedInt.has_value())
    {
        return static_cast<double>(*signedInt);
    }
    if (const auto* u = value.tryGetAs<uint64_t>())
    {
        return static_cast<double>(*u);
    }
    return std::unexpected{InvalidQuerySyntax("Expected numeric NESValue")};
}

}
