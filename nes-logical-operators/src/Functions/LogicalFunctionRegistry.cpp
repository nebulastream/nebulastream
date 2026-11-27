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

#include <LogicalFunctionRegistry.hpp>

#include <cstddef>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/ScalarTypes.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Types/NESType.hpp>
#include <DataTypeRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// NOLINTBEGIN(bugprone-throwing-static-initialization)
const NESFunctorParameter FUNCTION{
    Identifier::parse("FUNCTION"),
    "Binary function functor required to be closed over the constrained type"};

const NESTypeParameter INPUT_TYPE{
    Identifier::parse("INPUT_TYPE"),
    "Input scalar type of the unary function",
    &DataTypeRegistry::scalarDataTypeClass};

const NESTypeParameter LEFT_TYPE{
    Identifier::parse("LEFT_TYPE"),
    "Left operand scalar type of the binary function",
    &DataTypeRegistry::scalarDataTypeClass};

const NESTypeParameter RIGHT_TYPE{
    Identifier::parse("RIGHT_TYPE"),
    "Right operand scalar type of the binary function",
    &DataTypeRegistry::scalarDataTypeClass};
/// NOLINTEND(bugprone-throwing-static-initialization)

struct ConstructibleLogicalFunctionNESType
{
    std::shared_ptr<const int> identityToken;
    bool sqlCallable = true;
    std::string diagnosticName;
    std::vector<NESType> instantiatedArgTypes;
    std::optional<NESType> inferredResultType;
    LogicalFunctionFn factory;

    bool operator==(const ConstructibleLogicalFunctionNESType& other) const
    {
        return identityToken == other.identityToken && instantiatedArgTypes == other.instantiatedArgTypes
            && inferredResultType == other.inferredResultType;
    }

    [[nodiscard]] std::expected<LogicalFunction, Exception> construct(const NESValueParameters& params) const
    {
        if (!sqlCallable)
        {
            return std::unexpected{
                InvalidQuerySyntax("Function '{}' cannot be called directly as a SQL function", diagnosticName)};
        }
        if (params.isNamed())
        {
            return std::unexpected{
                InvalidQuerySyntax("Scalar function '{}' does not accept named arguments", diagnosticName)};
        }
        std::vector<LogicalFunction> children;
        children.reserve(params.size());
        for (size_t i = 0; i < params.size(); ++i)
        {
            auto childExp = nesValueToLogicalFunction(params.at(i).value());
            if (!childExp.has_value())
            {
                return std::unexpected{std::move(childExp).error()};
            }
            children.push_back(std::move(*childExp));
        }
        try
        {
            return factory(LogicalFunctionRegistryArguments{.children = std::move(children)});
        }
        catch (const Exception& ex)
        {
            return std::unexpected{ex};
        }
    }
};

struct BoundLogicalFunctionNESType
{
    using ValueType = LogicalFunction;

    std::string functionKey;
    std::optional<NESType> inferredResultType;

    bool operator==(const BoundLogicalFunctionNESType& other) const = default;
};

NESType makeExpressionNESType(
    std::shared_ptr<const int> identityToken,
    const bool sqlCallable,
    std::string diagnosticName,
    std::vector<NESType> instantiatedArgTypes,
    std::optional<NESType> inferredResult,
    LogicalFunctionFn factory)
{
    return NESType{ConstructibleLogicalFunctionNESType{
                       .identityToken = std::move(identityToken),
                       .sqlCallable = sqlCallable,
                       .diagnosticName = std::move(diagnosticName),
                       .instantiatedArgTypes = std::move(instantiatedArgTypes),
                       .inferredResultType = inferredResult,
                       .factory = std::move(factory)}}
        .withTypeTrait(LogicalFunctionTypeTrait{.resultScalarType = std::move(inferredResult)});
}

struct ClosedBinaryFunctionTypeClass
{
    NESFunctor functionFunctor;
    bool operator==(const ClosedBinaryFunctionTypeClass&) const = default;

    [[nodiscard]] bool validate(const NESType& candidate) const
    {
        if (!DataTypeRegistry::scalarDataTypeClass().validate(candidate))
        {
            return false;
        }
        const auto instantiatedExp = functionFunctor.instantiate(NESTypeParameters::fromTypes({candidate, candidate}));
        if (!instantiatedExp.has_value())
        {
            return false;
        }
        if (const auto* fnTrait = instantiatedExp->tryGetTypeTrait<LogicalFunctionTypeTrait>();
            fnTrait != nullptr && fnTrait->resultScalarType.has_value())
        {
            return *fnTrait->resultScalarType == candidate;
        }
        return *instantiatedExp == candidate;
    }
};

struct ClosedBinaryParametrizedTypeClass
{
    bool operator==(const ClosedBinaryParametrizedTypeClass&) const = default;

    [[nodiscard]] static ExpectedNESTypeArguments getExpectedArguments()
    {
        return createExpectedNESTypeArguments(FUNCTION);
    }

    [[nodiscard]] static std::expected<NESTypeClass, Exception> instantiate(const InstantiatedNESTypeParameters& params)
    {
        return LogicalFunctionRegistry::closedBinaryFunctionTypeClass(params.get(FUNCTION));
    }
};

struct LogicalFunctionFunctor
{
    std::shared_ptr<const int> identityToken;
    bool sqlCallable = true;
    std::string diagnosticName;
    LogicalFunctionTypeInference typeInference;
    LogicalFunctionFn factory;

    [[nodiscard]] const LogicalFunctionFn& getRegistryEntry() const noexcept { return factory; }

    bool operator==(const LogicalFunctionFunctor& other) const
    {
        return identityToken == other.identityToken;
    }

    [[nodiscard]] std::expected<NESType, Exception> instantiate(const NESTypeParameters& typeParams) const
    {
        if (typeParams.empty())
        {
            return makeExpressionNESType(identityToken, sqlCallable, diagnosticName, {}, std::nullopt, factory);
        }
        if (!sqlCallable || std::holds_alternative<std::monostate>(typeInference))
        {
            return std::unexpected{
                TypeInferenceException("Internal function '{}' cannot be instantiated over type arguments", diagnosticName)};
        }

        if (const auto* unaryFn = std::get_if<UnaryTypeInferenceFn>(&typeInference))
        {
            const auto resolvedExp = createExpectedNESTypeArguments(INPUT_TYPE).resolve(typeParams);
            if (!resolvedExp.has_value())
            {
                return std::unexpected{resolvedExp.error()};
            }
            const auto& inputType = resolvedExp->get(INPUT_TYPE);
            auto inferredExp = (*unaryFn)(inputType);
            if (!inferredExp.has_value())
            {
                return std::unexpected{std::move(inferredExp).error()};
            }
            return makeExpressionNESType(identityToken, sqlCallable, diagnosticName, {inputType}, std::move(*inferredExp), factory);
        }

        if (const auto* binaryFn = std::get_if<BinaryTypeInferenceFn>(&typeInference))
        {
            const auto resolvedExp = createExpectedNESTypeArguments(LEFT_TYPE, RIGHT_TYPE).resolve(typeParams);
            if (!resolvedExp.has_value())
            {
                return std::unexpected{resolvedExp.error()};
            }
            const auto& leftType = resolvedExp->get(LEFT_TYPE);
            const auto& rightType = resolvedExp->get(RIGHT_TYPE);
            auto inferredExp = (*binaryFn)(leftType, rightType);
            if (!inferredExp.has_value())
            {
                return std::unexpected{std::move(inferredExp).error()};
            }
            return makeExpressionNESType(
                identityToken, sqlCallable, diagnosticName, {leftType, rightType}, std::move(*inferredExp), factory);
        }

        const auto& variadicFn = std::get<VariadicTypeInferenceFn>(typeInference);
        if (typeParams.isNamed())
        {
            return std::unexpected{
                TypeInferenceException("Variadic function '{}' expects positional type arguments", diagnosticName)};
        }
        std::vector<NESType> argTypes;
        argTypes.reserve(typeParams.size());
        for (size_t i = 0; i < typeParams.size(); ++i)
        {
            const auto argTypeOpt = typeParams.getTypeAt(i);
            if (!argTypeOpt.has_value())
            {
                return std::unexpected{
                    TypeInferenceException("Function '{}' expects type parameters at index {}", diagnosticName, i)};
            }
            if (!DataTypeRegistry::scalarDataTypeClass().validate(*argTypeOpt))
            {
                return std::unexpected{TypeInferenceException(
                    "Function '{}' argument {} does not satisfy ScalarDatatype", diagnosticName, i)};
            }
            argTypes.push_back(*argTypeOpt);
        }
        auto inferredExp = (*variadicFn)(argTypes);
        if (!inferredExp.has_value())
        {
            return std::unexpected{std::move(inferredExp).error()};
        }
        return makeExpressionNESType(
            identityToken, sqlCallable, diagnosticName, std::move(argTypes), std::move(*inferredExp), factory);
    }
};
}

std::expected<LogicalFunction, Exception> nesValueToLogicalFunction(const NESValue& value)
{
    if (const auto* fn = value.tryGetAs<LogicalFunction>())
    {
        return *fn;
    }
    if (DataTypeRegistry::scalarDataTypeClass().validate(value.getType()))
    {
        return ConstantValueLogicalFunction::tryCreate(value).transform(
            [](ConstantValueLogicalFunction&& constFn) { return LogicalFunction{std::move(constFn)}; });
    }
    return std::unexpected{InvalidQuerySyntax("Expected a scalar expression or literal argument")};
}

NESValue makeLogicalFunctionValue(LogicalFunction function)
{
    const auto dt = function.getDataType();
    std::optional<NESType> resultType;
    if (dt.type != DataType::Type::UNDEFINED)
    {
        resultType = dt.toNESType();
    }
    auto exprType = NESType{BoundLogicalFunctionNESType{
                                .functionKey = std::string{function.getType()},
                                .inferredResultType = resultType}}
                        .withTypeTrait(LogicalFunctionTypeTrait{.resultScalarType = resultType});
    return NESValue::create(std::move(exprType), std::move(function));
}

std::expected<NESType, Exception>
joinNumericNESTypes(const NESType& left, const NESType& right, const std::string_view functionName)
{
    if (!left.hasTypeTrait<NumericTypeTrait>() || !right.hasTypeTrait<NumericTypeTrait>())
    {
        return std::unexpected{CannotInferStamp("Cannot apply {} to non-numeric input types", functionName)};
    }
    const auto* leftLegacy = left.tryGetTypeTrait<LegacyDataTypeTrait>();
    const auto* rightLegacy = right.tryGetTypeTrait<LegacyDataTypeTrait>();
    if (leftLegacy == nullptr || rightLegacy == nullptr)
    {
        return std::unexpected{
            CannotInferStamp("Built-in function '{}' does not have an overload for custom numeric types", functionName)};
    }
    const auto joined = leftLegacy->getDataType().join(rightLegacy->getDataType());
    if (!joined.has_value())
    {
        return std::unexpected{CannotInferStamp(
            "Could not join data types in {}: left={}, right={}", functionName, leftLegacy->getDataType(), rightLegacy->getDataType())};
    }
    return joined->toNESType();
}

DataType requireLegacyDataType(std::expected<NESType, Exception> inferredType)
{
    if (!inferredType.has_value())
    {
        throw std::move(inferredType).error();
    }
    const auto* legacyTrait = inferredType->tryGetTypeTrait<LegacyDataTypeTrait>();
    INVARIANT(legacyTrait != nullptr, "Expected LegacyDataTypeTrait on inferred built-in scalar NESType");
    return legacyTrait->getDataType();
}

LogicalFunctionRegistry& LogicalFunctionRegistry::instance()
{
    static LogicalFunctionRegistry inst;
    return inst;
}

NESTypeClass LogicalFunctionRegistry::logicalFunctionTypeClass()
{
    return NESTypeClass::forTypeTrait<LogicalFunctionTypeTrait>() || DataTypeRegistry::scalarDataTypeClass();
}

NESTypeClass LogicalFunctionRegistry::closedBinaryFunctionTypeClass(NESFunctor functionFunctor)
{
    return NESTypeClass{ClosedBinaryFunctionTypeClass{.functionFunctor = std::move(functionFunctor)}};
}

std::vector<NamedNESParametrizedTypeClass> LogicalFunctionRegistry::getParametrizedTypeClasses()
{
    return {
        NamedNESParametrizedTypeClass::constant(QualifiedIdentifier::parse("LogicalFunction"), logicalFunctionTypeClass()),
        NamedNESParametrizedTypeClass{
            QualifiedIdentifier::parse("ClosedBinaryFunction"),
            NESParametrizedTypeClass{ClosedBinaryParametrizedTypeClass{}}}};
}

namespace detail
{
NESFunctor makeLogicalFunctionFunctorImpl(
    LogicalFunctionFn factory,
    LogicalFunctionTypeInference typeInference,
    const bool sqlCallable,
    const std::string_view diagnosticName)
{
    return NESFunctor{LogicalFunctionFunctor{
        .identityToken = std::make_shared<const int>(0),
        .sqlCallable = sqlCallable,
        .diagnosticName = std::string{diagnosticName},
        .typeInference = typeInference,
        .factory = std::move(factory)}};
}
}

}
