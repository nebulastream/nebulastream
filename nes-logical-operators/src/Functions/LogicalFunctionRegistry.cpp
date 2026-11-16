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

#include <array>
#include <cstdint>
#include <expected>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Types/NESType.hpp>
#include <Types/NESTypeCatalog.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>
#include <DataTypeRegistry.hpp>
#include <ErrorHandling.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

namespace NES
{

namespace
{
struct LogicalFunctionDescriptor
{
    std::string functionKey;
    std::optional<NESType> inferredResultType;
    LogicalFunctionFn factory;

    bool operator==(const LogicalFunctionDescriptor& other) const
    {
        return toUpperCase(functionKey) == toUpperCase(other.functionKey) && inferredResultType == other.inferredResultType;
    }
};

struct ClosedBinaryFunctionDescriptor
{
    QualifiedIdentifier functionName;
    bool operator==(const ClosedBinaryFunctionDescriptor&) const = default;
};

struct ClosedBinaryParametrizedTag
{
    bool operator==(const ClosedBinaryParametrizedTag&) const = default;
};

bool isInternalAstOnlyFunction(const std::string_view key)
{
    const auto upper = toUpperCase(key);
    return upper == "CONSTANTVALUE" || upper == "FIELDACCESS" || upper == "UNBOUNDFIELDACCESS" || upper == "CASTTOTYPE"
        || upper == "VARSIZEDTONUMERIC";
}

std::string scalarLiteralToConstantString(const ScalarLiteralValue& literal)
{
    return std::visit(
        Overloaded{
            [](const std::string& s) { return s; },
            [](const int64_t i) { return std::to_string(i); },
            [](const uint64_t u) { return std::to_string(u); },
            [](const double d) { return fmt::format("{}", d); },
            [](const bool b) -> std::string { return b ? "true" : "false"; },
            [](const std::monostate) -> std::string { return "NULL"; }},
        literal);
}

std::expected<QualifiedIdentifier, Exception> extractFunctionNameFromParam(const NESTypeParameters& params)
{
    if (params.size() != 1)
    {
        return std::unexpected{
            TypeInferenceException("ClosedBinaryFunction expects exactly 1 parameter (FUNCTION_NAME), got {}", params.size())};
    }
    const auto valOpt
        = params.isNamed() ? params.getValueByName(QualifiedIdentifier::parse("FUNCTION_NAME")) : params.getValueAt(0);
    if (!valOpt.has_value())
    {
        return std::unexpected{
            TypeInferenceException("ClosedBinaryFunction expects a value parameter naming the binary function")};
    }
    if (const auto* qid = valOpt->tryGetAs<QualifiedIdentifier>())
    {
        return *qid;
    }
    if (const auto* s = valOpt->tryGetAs<std::string>())
    {
        return QualifiedIdentifier::tryParse(*s);
    }
    if (const auto* lit = valOpt->tryGetAs<ScalarLiteralValue>())
    {
        if (const auto* s = std::get_if<std::string>(lit))
        {
            return QualifiedIdentifier::tryParse(*s);
        }
    }
    return std::unexpected{TypeInferenceException("ClosedBinaryFunction parameter must be a function identifier or string")};
}

NESType makeExpressionNESType(std::string key, std::optional<NESType> inferredResult, LogicalFunctionFn factory)
{
    return NESType::create<LogicalFunctionDescriptor, LogicalFunction>(
               LogicalFunctionDescriptor{
                   .functionKey = std::move(key),
                   .inferredResultType = inferredResult,
                   .factory = std::move(factory)},
               [](const LogicalFunctionDescriptor& desc, const NESValueParameters& params)
                   -> std::expected<LogicalFunction, Exception>
               {
                   if (isInternalAstOnlyFunction(desc.functionKey))
                   {
                       return std::unexpected{
                           InvalidQuerySyntax("Function '{}' cannot be called directly as a SQL function", desc.functionKey)};
                   }
                   if (params.isNamed())
                   {
                       return std::unexpected{
                           InvalidQuerySyntax("Scalar function '{}' does not accept named arguments", desc.functionKey)};
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
                       return desc.factory(LogicalFunctionRegistryArguments{.children = std::move(children)});
                   }
                   catch (const Exception& ex)
                   {
                       return std::unexpected{ex};
                   }
               })
        .withTrait(LogicalFunctionTypeTrait{.resultScalarType = std::move(inferredResult)});
}
}

std::expected<LogicalFunction, Exception> nesValueToLogicalFunction(const NESValue& value)
{
    if (const auto* fn = value.tryGetAs<LogicalFunction>())
    {
        return *fn;
    }
    if (const auto* trait = value.getType().tryGetTrait<ScalarTypeTrait>())
    {
        if (const auto dtOpt = trait->tryGetDataType(value.getType().getDescriptor()))
        {
            if (const auto* lit = value.tryGetAs<ScalarLiteralValue>())
            {
                return LogicalFunction{ConstantValueLogicalFunction{*dtOpt, scalarLiteralToConstantString(*lit)}};
            }
            if (const auto* s = value.tryGetAs<std::string>())
            {
                return LogicalFunction{ConstantValueLogicalFunction{*dtOpt, *s}};
            }
        }
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
    auto exprType = NESType::createUnconstructible<LogicalFunctionDescriptor, LogicalFunction>(LogicalFunctionDescriptor{
                        .functionKey = std::string{function.getType()},
                        .inferredResultType = resultType,
                        .factory = {}})
                        .withTrait(LogicalFunctionTypeTrait{.resultScalarType = resultType});
    return NESValue::create(std::move(exprType), std::move(function));
}

LogicalFunctionRegistry& LogicalFunctionRegistry::instance()
{
    static LogicalFunctionRegistry inst;
    return inst;
}

NESTypeClass LogicalFunctionRegistry::logicalFunctionTypeClass()
{
    return NESTypeClass::forTrait<LogicalFunctionTypeTrait>() || DataTypeRegistry::scalarDataTypeClass();
}

NESTypeClass LogicalFunctionRegistry::closedBinaryFunctionTypeClass(QualifiedIdentifier functionName)
{
    return NESTypeClass{
        ClosedBinaryFunctionDescriptor{.functionName = std::move(functionName)},
        [](const ClosedBinaryFunctionDescriptor& desc, const NESType& candidate)
        {
            if (!DataTypeRegistry::scalarDataTypeClass().validate(candidate))
            {
                return false;
            }
            auto functorOpt = NESTypeCatalog::instance().findSqlFunctor(desc.functionName);
            if (!functorOpt.has_value())
            {
                const auto flatName = fmt::format(
                    "{}",
                    fmt::join(
                        desc.functionName | std::views::transform([](const Identifier& part) { return part.getOriginalString(); }), "."));
                functorOpt = LogicalFunctionRegistry::instance().findFunctor(flatName);
            }
            if (!functorOpt.has_value())
            {
                return false;
            }
            const auto instantiatedExp = functorOpt->instantiate(NESTypeParameters::fromTypes({candidate, candidate}));
            if (!instantiatedExp.has_value())
            {
                return false;
            }
            if (const auto* fnTrait = instantiatedExp->tryGetTrait<LogicalFunctionTypeTrait>();
                fnTrait != nullptr && fnTrait->resultScalarType.has_value())
            {
                return *fnTrait->resultScalarType == candidate;
            }
            return *instantiatedExp == candidate;
        }};
}

std::vector<NamedNESParametrizedTypeClass> LogicalFunctionRegistry::getParametrizedTypeClasses()
{
    return {
        NamedNESParametrizedTypeClass::constant(QualifiedIdentifier::parse("LogicalFunction"), logicalFunctionTypeClass()),
        NamedNESParametrizedTypeClass{
            QualifiedIdentifier::parse("ClosedBinaryFunction"),
            NESParametrizedTypeClass{
                ClosedBinaryParametrizedTag{},
                [](const ClosedBinaryParametrizedTag&, const NESTypeClassParameters& params) -> std::expected<NESTypeClass, Exception>
                {
                    auto fnNameExp = extractFunctionNameFromParam(params.getParameters());
                    if (!fnNameExp.has_value())
                    {
                        return std::unexpected{std::move(fnNameExp).error()};
                    }
                    return closedBinaryFunctionTypeClass(std::move(*fnNameExp));
                }}}};
}

NESFunctor makeLogicalFunctionFunctor(const std::string_view key, LogicalFunctionFn factory)
{
    const std::string keyCopy{key};
    return NESFunctor{
        std::move(factory),
        [](const LogicalFunctionFn& lhs, const LogicalFunctionFn& rhs) { return &lhs == &rhs; },
        [keyCopy](const LogicalFunctionFn& fn, const NESTypeParameters& typeParams) -> std::expected<NESType, Exception>
        {
            if (typeParams.empty())
            {
                return makeExpressionNESType(keyCopy, std::nullopt, fn);
            }
            if (isInternalAstOnlyFunction(keyCopy))
            {
                return std::unexpected{
                    TypeInferenceException("Internal function '{}' cannot be instantiated over type arguments", keyCopy)};
            }
            if (typeParams.isNamed())
            {
                return std::unexpected{
                    TypeInferenceException("Function '{}' expects positional type arguments", keyCopy)};
            }

            const LogicalOperator dummyProducer{SourceNameLogicalOperator::create(Identifier::parse("_type_probe"))};
            std::vector<Field> dummyFields;
            std::vector<LogicalFunction> dummyChildren;
            dummyFields.reserve(typeParams.size());
            dummyChildren.reserve(typeParams.size());

            for (size_t i = 0; i < typeParams.size(); ++i)
            {
                const auto argTypeOpt = typeParams.getTypeAt(i);
                if (!argTypeOpt.has_value())
                {
                    return std::unexpected{
                        TypeInferenceException("Function '{}' expects type parameters, got value at index {}", keyCopy, i)};
                }
                const auto* scalarTrait = argTypeOpt->tryGetTrait<ScalarTypeTrait>();
                if (scalarTrait == nullptr)
                {
                    return std::unexpected{TypeInferenceException(
                        "Function '{}' argument {} does not satisfy ScalarDatatype", keyCopy, i)};
                }
                const auto dtOpt = scalarTrait->tryGetDataType(argTypeOpt->getDescriptor());
                if (!dtOpt.has_value())
                {
                    return std::unexpected{TypeInferenceException(
                        "Built-in function '{}' does not have an overload for custom non-DataType scalar argument {}", keyCopy, i)};
                }
                Field dummyField{dummyProducer, Identifier::parse(fmt::format("_arg_{}", i)), *dtOpt};
                dummyFields.push_back(dummyField);
                dummyChildren.emplace_back(FieldAccessLogicalFunction{dummyField});
            }

            try
            {
                const Schema<Field, Unordered> dummySchema{std::move(dummyFields)};
                auto constructed = fn(LogicalFunctionRegistryArguments{.children = std::move(dummyChildren)});
                auto inferred = constructed.withInferredDataType(dummySchema);
                return makeExpressionNESType(keyCopy, inferred.getDataType().toNESType(), fn);
            }
            catch (const Exception& ex)
            {
                return std::unexpected{ex};
            }
        }};
}

}
