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

#include <SQLQueryParser/StatementBinder.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <ANTLRInputStream.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <BailErrorStrategy.h>
#include <BaseErrorListener.h>
#include <CommonTokenStream.h>
#include <Exceptions.h>
#include <Recognizer.h>
#include <Token.h>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/ModuloLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/MulLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/BooleanFunctions/IsNaNLogicalFunction.hpp>
#include <Functions/BooleanFunctions/IsNullCheckLogicalFunction.hpp>
#include <Functions/BooleanFunctions/NegateLogicalFunction.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/AnonymousSinkLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/AnonymousSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Types/NESType.hpp>
#include <Types/NESTypeCatalog.hpp>
#include <Util/Optional.hpp>
#include <Util/Overloaded.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>
#include <Util/Strings.hpp>
#include <Util/URI.hpp>
#include <Util/Variant.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <DataTypeRegistry.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <Model.hpp>
#include <ModelCatalog.hpp>
#include <ModelRegistry.hpp>
#include <ParserUtil.hpp>
#include <SinkConfigSchemaRegistry.hpp>
#include <SourceConfigSchemaRegistry.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

namespace NES
{

namespace
{
using Literal = ScalarLiteralValue;
using ConfigMap
    = std::unordered_map<Identifier, std::unordered_map<Identifier, std::variant<Literal, Schema<UnqualifiedUnboundField, Ordered>>>>;
using ConfigMultiMap = std::vector<std::pair<QualifiedIdentifier, std::variant<Literal, Schema<UnqualifiedUnboundField, Ordered>>>>;

class ThrowingSyntaxErrorListener final : public antlr4::BaseErrorListener
{
public:
    void syntaxError(
        antlr4::Recognizer*,
        antlr4::Token*,
        const size_t line,
        const size_t charPositionInLine,
        const std::string& msg,
        std::exception_ptr) override
    {
        throw InvalidQuerySyntax("Syntax error at {}:{}: {}", line, charPositionInLine, msg);
    }
};

struct TimeInterval
{
    Windowing::TimeMeasure measure;
    bool operator==(const TimeInterval& other) const = default;
};

struct UnaryWindowDefinition
{
    Windowing::TimeBasedWindowType windowType;
    Windowing::UnboundTimeCharacteristic timeCharacteristic;
};

struct BinaryWindowDefinition
{
    Windowing::TimeBasedWindowType windowType;
    std::array<Windowing::UnboundTimeCharacteristic, 2> timeCharacteristics;
};

struct TimeIntervalTag
{
    bool operator==(const TimeIntervalTag&) const = default;
};

struct UnaryWindowTag
{
    bool operator==(const UnaryWindowTag&) const = default;
};

struct BinaryWindowTag
{
    bool operator==(const BinaryWindowTag&) const = default;
};

struct SchemaValueTag
{
    bool operator==(const SchemaValueTag&) const = default;
};

NESValue makeTimeIntervalValue(TimeInterval interval)
{
    static const NESType intervalType = NESType::createUnconstructible<TimeIntervalTag, TimeInterval>(TimeIntervalTag{});
    return NESValue::create(intervalType, interval);
}

NESValue makeSchemaNESValue(Schema<UnqualifiedUnboundField, Ordered> schema)
{
    static const NESType schemaType
        = NESType::createUnconstructible<SchemaValueTag, Schema<UnqualifiedUnboundField, Ordered>>(SchemaValueTag{});
    return NESValue::create(schemaType, std::move(schema));
}

NESValue makeScalarLiteralNESValue(Literal lit)
{
    static const NESType rawLitType
        = DataType{DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE}.toNESType();
    return NESValue::create(rawLitType, std::move(lit));
}

std::expected<TypedLogicalFunction<UnboundFieldAccessLogicalFunction>, Exception>
extractTimestampField(const NESValue& val, const std::string_view windowKind)
{
    if (const auto* fn = val.tryGetAs<LogicalFunction>())
    {
        if (const auto unbound = fn->tryGetAs<UnboundFieldAccessLogicalFunction>())
        {
            return *unbound;
        }
    }
    return std::unexpected{InvalidQuerySyntax("{} window timestamp argument must be a field identifier", windowKind)};
}

NESFunctor makeUnaryTumblingFunctor()
{
    return NESFunctor::nullary<UnaryWindowTag>(
        UnaryWindowTag{},
        [](const UnaryWindowTag&) -> std::expected<NESType, Exception>
        {
            return NESType::create<UnaryWindowTag, UnaryWindowDefinition>(
                UnaryWindowTag{},
                [](const UnaryWindowTag&, const NESValueParameters& params) -> std::expected<UnaryWindowDefinition, Exception>
                {
                    if (params.isNamed())
                    {
                        return std::unexpected{InvalidQuerySyntax("TUMBLING window does not accept named arguments")};
                    }
                    if (params.size() == 1)
                    {
                        const auto arg0 = params.at(0).value();
                        const auto* sizePtr = arg0.tryGetAs<TimeInterval>();
                        if (sizePtr == nullptr)
                        {
                            return std::unexpected{InvalidQuerySyntax("TUMBLING(interval) expects a time interval")};
                        }
                        return UnaryWindowDefinition{
                            .windowType = Windowing::TimeBasedWindowType{Windowing::TumblingWindow{sizePtr->measure}},
                            .timeCharacteristic = Windowing::IngestionTimeCharacteristic{}};
                    }
                    if (params.size() == 2)
                    {
                        auto tsExp = extractTimestampField(params.at(0).value(), "Unary TUMBLING");
                        if (!tsExp.has_value())
                        {
                            return std::unexpected{tsExp.error()};
                        }
                        const auto arg1 = params.at(1).value();
                        const auto* sizePtr = arg1.tryGetAs<TimeInterval>();
                        if (sizePtr == nullptr)
                        {
                            return std::unexpected{InvalidQuerySyntax("TUMBLING(timestamp, interval) expects a time interval")};
                        }
                        return UnaryWindowDefinition{
                            .windowType = Windowing::TimeBasedWindowType{Windowing::TumblingWindow{sizePtr->measure}},
                            .timeCharacteristic = Windowing::UnboundEventTimeCharacteristic{.field = *tsExp}};
                    }
                    return std::unexpected{
                        InvalidQuerySyntax("Unary TUMBLING window expects 1 or 2 arguments, got {}", params.size())};
                });
        });
}

NESFunctor makeUnarySlidingFunctor()
{
    return NESFunctor::nullary<UnaryWindowTag>(
        UnaryWindowTag{},
        [](const UnaryWindowTag&) -> std::expected<NESType, Exception>
        {
            return NESType::create<UnaryWindowTag, UnaryWindowDefinition>(
                UnaryWindowTag{},
                [](const UnaryWindowTag&, const NESValueParameters& params) -> std::expected<UnaryWindowDefinition, Exception>
                {
                    if (params.isNamed())
                    {
                        return std::unexpected{InvalidQuerySyntax("SLIDING window does not accept named arguments")};
                    }
                    if (params.size() == 2)
                    {
                        const auto arg0 = params.at(0).value();
                        const auto arg1 = params.at(1).value();
                        const auto* sizePtr = arg0.tryGetAs<TimeInterval>();
                        const auto* slidePtr = arg1.tryGetAs<TimeInterval>();
                        if (sizePtr == nullptr || slidePtr == nullptr)
                        {
                            return std::unexpected{InvalidQuerySyntax("SLIDING(size, advance) expects two time intervals")};
                        }
                        return UnaryWindowDefinition{
                            .windowType = Windowing::TimeBasedWindowType{Windowing::SlidingWindow(sizePtr->measure, slidePtr->measure)},
                            .timeCharacteristic = Windowing::IngestionTimeCharacteristic{}};
                    }
                    if (params.size() == 3)
                    {
                        auto tsExp = extractTimestampField(params.at(0).value(), "Unary SLIDING");
                        if (!tsExp.has_value())
                        {
                            return std::unexpected{tsExp.error()};
                        }
                        const auto arg1 = params.at(1).value();
                        const auto arg2 = params.at(2).value();
                        const auto* sizePtr = arg1.tryGetAs<TimeInterval>();
                        const auto* slidePtr = arg2.tryGetAs<TimeInterval>();
                        if (sizePtr == nullptr || slidePtr == nullptr)
                        {
                            return std::unexpected{
                                InvalidQuerySyntax("SLIDING(timestamp, size, advance) expects two time intervals")};
                        }
                        return UnaryWindowDefinition{
                            .windowType = Windowing::TimeBasedWindowType{Windowing::SlidingWindow(sizePtr->measure, slidePtr->measure)},
                            .timeCharacteristic = Windowing::UnboundEventTimeCharacteristic{.field = *tsExp}};
                    }
                    return std::unexpected{
                        InvalidQuerySyntax("Unary SLIDING window expects 2 or 3 arguments, got {}", params.size())};
                });
        });
}

NESFunctor makeBinaryTumblingFunctor()
{
    return NESFunctor::nullary<BinaryWindowTag>(
        BinaryWindowTag{},
        [](const BinaryWindowTag&) -> std::expected<NESType, Exception>
        {
            return NESType::create<BinaryWindowTag, BinaryWindowDefinition>(
                BinaryWindowTag{},
                [](const BinaryWindowTag&, const NESValueParameters& params) -> std::expected<BinaryWindowDefinition, Exception>
                {
                    if (params.isNamed())
                    {
                        return std::unexpected{InvalidQuerySyntax("Join TUMBLING window does not accept named arguments")};
                    }
                    if (params.size() == 1)
                    {
                        const auto arg0 = params.at(0).value();
                        const auto* sizePtr = arg0.tryGetAs<TimeInterval>();
                        if (sizePtr == nullptr)
                        {
                            return std::unexpected{InvalidQuerySyntax("BINARY_WINDOW TUMBLING(interval) expects a time interval")};
                        }
                        return BinaryWindowDefinition{
                            .windowType = Windowing::TimeBasedWindowType{Windowing::TumblingWindow{sizePtr->measure}},
                            .timeCharacteristics
                            = {Windowing::IngestionTimeCharacteristic{}, Windowing::IngestionTimeCharacteristic{}}};
                    }
                    if (params.size() == 3)
                    {
                        auto leftTsExp = extractTimestampField(params.at(0).value(), "Binary TUMBLING");
                        if (!leftTsExp.has_value())
                        {
                            return std::unexpected{leftTsExp.error()};
                        }
                        auto rightTsExp = extractTimestampField(params.at(1).value(), "Binary TUMBLING");
                        if (!rightTsExp.has_value())
                        {
                            return std::unexpected{rightTsExp.error()};
                        }
                        const auto arg2 = params.at(2).value();
                        const auto* sizePtr = arg2.tryGetAs<TimeInterval>();
                        if (sizePtr == nullptr)
                        {
                            return std::unexpected{
                                InvalidQuerySyntax("BINARY_WINDOW TUMBLING(ts1, ts2, interval) expects a time interval")};
                        }
                        return BinaryWindowDefinition{
                            .windowType = Windowing::TimeBasedWindowType{Windowing::TumblingWindow{sizePtr->measure}},
                            .timeCharacteristics = {
                                Windowing::UnboundEventTimeCharacteristic{.field = *leftTsExp},
                                Windowing::UnboundEventTimeCharacteristic{.field = *rightTsExp}}};
                    }
                    return std::unexpected{
                        InvalidQuerySyntax("Join TUMBLING window expects 1 or 3 arguments, got {}", params.size())};
                });
        });
}

NESFunctor makeBinarySlidingFunctor()
{
    return NESFunctor::nullary<BinaryWindowTag>(
        BinaryWindowTag{},
        [](const BinaryWindowTag&) -> std::expected<NESType, Exception>
        {
            return NESType::create<BinaryWindowTag, BinaryWindowDefinition>(
                BinaryWindowTag{},
                [](const BinaryWindowTag&, const NESValueParameters& params) -> std::expected<BinaryWindowDefinition, Exception>
                {
                    if (params.isNamed())
                    {
                        return std::unexpected{InvalidQuerySyntax("Join SLIDING window does not accept named arguments")};
                    }
                    if (params.size() == 2)
                    {
                        const auto arg0 = params.at(0).value();
                        const auto arg1 = params.at(1).value();
                        const auto* sizePtr = arg0.tryGetAs<TimeInterval>();
                        const auto* slidePtr = arg1.tryGetAs<TimeInterval>();
                        if (sizePtr == nullptr || slidePtr == nullptr)
                        {
                            return std::unexpected{
                                InvalidQuerySyntax("BINARY_WINDOW SLIDING(size, advance) expects two time intervals")};
                        }
                        return BinaryWindowDefinition{
                            .windowType = Windowing::TimeBasedWindowType{Windowing::SlidingWindow(sizePtr->measure, slidePtr->measure)},
                            .timeCharacteristics
                            = {Windowing::IngestionTimeCharacteristic{}, Windowing::IngestionTimeCharacteristic{}}};
                    }
                    if (params.size() == 4)
                    {
                        auto leftTsExp = extractTimestampField(params.at(0).value(), "Binary SLIDING");
                        if (!leftTsExp.has_value())
                        {
                            return std::unexpected{leftTsExp.error()};
                        }
                        auto rightTsExp = extractTimestampField(params.at(1).value(), "Binary SLIDING");
                        if (!rightTsExp.has_value())
                        {
                            return std::unexpected{rightTsExp.error()};
                        }
                        const auto arg2 = params.at(2).value();
                        const auto arg3 = params.at(3).value();
                        const auto* sizePtr = arg2.tryGetAs<TimeInterval>();
                        const auto* slidePtr = arg3.tryGetAs<TimeInterval>();
                        if (sizePtr == nullptr || slidePtr == nullptr)
                        {
                            return std::unexpected{
                                InvalidQuerySyntax("BINARY_WINDOW SLIDING(ts1, ts2, size, advance) expects two time intervals")};
                        }
                        return BinaryWindowDefinition{
                            .windowType = Windowing::TimeBasedWindowType{Windowing::SlidingWindow(sizePtr->measure, slidePtr->measure)},
                            .timeCharacteristics = {
                                Windowing::UnboundEventTimeCharacteristic{.field = *leftTsExp},
                                Windowing::UnboundEventTimeCharacteristic{.field = *rightTsExp}}};
                    }
                    return std::unexpected{
                        InvalidQuerySyntax("Join SLIDING window expects 2 or 4 arguments, got {}", params.size())};
                });
        });
}

using ScopeSymbol = std::variant<NESValue, NESFunctor>;

}

namespace detail
{
class NamedScopeEntry
{
    QualifiedIdentifier name;
    ScopeSymbol symbol;

public:
    NamedScopeEntry(QualifiedIdentifier name, ScopeSymbol symbol) : name(std::move(name)), symbol(std::move(symbol)) { }

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const { return name; }
    [[nodiscard]] const ScopeSymbol& getSymbol() const { return symbol; }
    [[nodiscard]] bool operator==(const NamedScopeEntry& other) const { return name == other.name && symbol == other.symbol; }
    friend std::ostream& operator<<(std::ostream& os, const NamedScopeEntry& entry)
    {
        return os << fmt::format("NamedScopeEntry({})", entry.name);
    }
};
}
}

template <>
struct std::hash<NES::detail::NamedScopeEntry>
{
    size_t operator()(const NES::detail::NamedScopeEntry& val) const noexcept
    {
        return std::hash<NES::QualifiedIdentifier>{}(val.getFullyQualifiedName());
    }
};

FMT_OSTREAM(NES::detail::NamedScopeEntry);

namespace NES
{
namespace
{
using detail::NamedScopeEntry;

class NestedScope
{
    std::vector<Schema<NamedScopeEntry, Unordered>> layers;

    explicit NestedScope(std::vector<Schema<NamedScopeEntry, Unordered>> layers) : layers(std::move(layers)) { }

public:
    static NestedScope create(Schema<NamedScopeEntry, Unordered> rootScope)
    {
        return NestedScope{{std::move(rootScope)}};
    }

    [[nodiscard]] NestedScope push(Schema<NamedScopeEntry, Unordered> childScope) const
    {
        auto next = layers;
        next.push_back(std::move(childScope));
        return NestedScope{std::move(next)};
    }

    [[nodiscard]] std::optional<ScopeSymbol> find(const QualifiedIdentifier& name) const
    {
        for (const auto& layer : std::views::reverse(layers))
        {
            if (const auto found = layer.getFieldByName(name))
            {
                return found->getSymbol();
            }
        }
        return std::nullopt;
    }
};

enum class ScalarClauseMode : uint8_t
{
    SelectTopLevel,
    SelectNested,
    Where,
    GroupBy,
    Window,
    Having,
    JoinOn
};

struct QueryBindState
{
    bool isJoinQuery = false;
    std::optional<Identifier> expectedQualifier;
    std::vector<Identifier> sourceQualifiers;

    std::vector<ProjectionLogicalOperator::UnboundProjection> preAggProjections;
    size_t aggExprCounter = 0;
    std::vector<std::pair<WindowAggregationLogicalFunction, Identifier>> windowAggs;
    std::vector<Identifier> userAggregationAliases;
    std::vector<UnboundFieldAccessLogicalFunction> groupByFields;

    bool lastTopLevelExprWasDirectAggregation = false;
    std::optional<WindowAggregationLogicalFunction> forbiddenHavingAggregation;
};

std::string describeAggregation(
    const WindowAggregationLogicalFunction& aggregation,
    const std::vector<ProjectionLogicalOperator::UnboundProjection>& desugaredArguments)
{
    const auto input = aggregation.getInputFunction();
    const auto field = [&]
    {
        if (const auto* unbound = std::get_if<TypedLogicalFunction<UnboundFieldAccessLogicalFunction>>(&input))
        {
            const auto desugared = std::ranges::find_if(
                desugaredArguments, [&unbound](const auto& projection) { return projection.first == (*unbound)->getFieldName(); });
            if (desugared != std::ranges::end(desugaredArguments))
            {
                return desugared->second.explain(ExplainVerbosity::Short);
            }
        }
        return std::visit([](const auto& access) { return access->explain(ExplainVerbosity::Short); }, input);
    }();
    return fmt::format("{}({})", toUpperCase(aggregation.getName()), field);
}

LogicalFunction createRawLiteralFunction(std::string literal)
{
    return ConstantValueLogicalFunction(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED), std::move(literal));
}

LogicalFunction createNegatedNumericLiteralFunction(const ConstantValueLogicalFunction& constantFunction)
{
    auto constantValue = constantFunction.getConstantValue();
    if (constantValue.starts_with('-'))
    {
        constantValue.erase(0, 1);
    }
    else
    {
        constantValue.insert(0, 1, '-');
    }
    return ConstantValueLogicalFunction(constantFunction.getDataType(), std::move(constantValue));
}

bool isDirectNumericLiteralAst(AntlrSQLParser::OpenScalarExpressionContext* expr)
{
    if (auto* litCtx = dynamic_cast<AntlrSQLParser::LiteralScalarExpressionContext*>(expr))
    {
        if (auto* scalarLit = litCtx->literal()->scalarLiteral())
        {
            return scalarLit->unsignedIntLiteral() != nullptr || scalarLit->floatLiteral() != nullptr;
        }
        return false;
    }
    if (auto* unaryCtx = dynamic_cast<AntlrSQLParser::UnaryArithmeticExpressionContext*>(expr))
    {
        return isDirectNumericLiteralAst(unaryCtx->openScalarExpression());
    }
    return false;
}

std::string unescapeSqlString(const std::string_view quoted)
{
    PRECONDITION(quoted.size() >= 2 && quoted.front() == '\'' && quoted.back() == '\'', "Expected single-quoted SQL string");
    const auto inner = quoted.substr(1, quoted.size() - 2);
    std::string out;
    out.reserve(inner.size());
    for (size_t i = 0; i < inner.size(); ++i)
    {
        if (inner[i] == '\'' && i + 1 < inner.size() && inner[i + 1] == '\'')
        {
            out.push_back('\'');
            ++i;
        }
        else
        {
            out.push_back(inner[i]);
        }
    }
    return out;
}

std::string normalizeBuiltinDataTypeName(std::string dataTypeText, bool* outTranslated = nullptr)
{
    bool translated = false;
    bool isUnsigned = false;
    const auto upper = toUpperCase(dataTypeText);
    if (upper.starts_with("UNSIGNED") && upper.size() > std::strlen("UNSIGNED")
        && std::isspace(static_cast<unsigned char>(upper[std::strlen("UNSIGNED")])) != 0)
    {
        isUnsigned = true;
        translated = true;
        dataTypeText = std::string{trimWhiteSpaces(upper.substr(std::strlen("UNSIGNED")))};
    }
    else
    {
        dataTypeText = upper;
    }

    static const std::unordered_map<std::string, std::string> DataTypeMapping
        = {{"TINYINT", "INT8"}, {"SMALLINT", "INT16"}, {"INT", "INT32"}, {"INTEGER", "INT32"}, {"BIGINT", "INT64"}};

    if (const auto found = DataTypeMapping.find(dataTypeText); found != DataTypeMapping.end())
    {
        translated = true;
        dataTypeText = isUnsigned ? ("U" + found->second) : found->second;
    }
    if (outTranslated != nullptr)
    {
        *outTranslated = translated;
    }
    return dataTypeText;
}

std::string literalToString(const Literal& literal)
{
    return std::visit(
        Overloaded{
            [](const std::string& str) { return str; },
            [](const int64_t val) { return std::to_string(val); },
            [](const uint64_t val) { return std::to_string(val); },
            [](const double val) { return fmt::format("{}", val); },
            [](const bool val) -> std::string { return val ? "true" : "false"; },
            [](const std::monostate) -> std::string { return "null"; }},
        literal);
}

Schema<UnqualifiedUnboundField, Ordered> normalizeNonNullableModelFields(const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    auto normalized = schema
        | std::views::transform(
                          [](const UnqualifiedUnboundField& field)
                          {
                              auto dt = field.getDataType();
                              dt.nullable = false;
                              return UnqualifiedUnboundField{field.getFullyQualifiedName(), dt};
                          })
        | std::ranges::to<std::vector>();
    return Schema<UnqualifiedUnboundField, Ordered>{std::move(normalized)};
}

}

/// NOLINTBEGIN(readability-convert-member-functions-to-static)
class StatementBinder::Impl
{
    std::shared_ptr<const SourceCatalog> sourceCatalog;
    std::shared_ptr<const SinkCatalog> sinkCatalog;
    std::shared_ptr<const ModelCatalog> modelCatalog;
    Schema<ConfigFieldDefault, Ordered> defaultConfigValues;
    Schema<ConfigFieldTransformation, Unordered> configTransformations;

public:
    Impl(
        std::shared_ptr<const SourceCatalog> sourceCatalog,
        std::shared_ptr<const SinkCatalog> sinkCatalog,
        std::shared_ptr<const ModelCatalog> modelCatalog,
        Schema<ConfigFieldDefault, Ordered> defaultConfigValues,
        Schema<ConfigFieldTransformation, Unordered> configTransformations)
        : sourceCatalog(std::move(sourceCatalog))
        , sinkCatalog(std::move(sinkCatalog))
        , modelCatalog(std::move(modelCatalog))
        , defaultConfigValues(std::move(defaultConfigValues))
        , configTransformations(std::move(configTransformations))
    {
    }

    ~Impl() = default;

    NestedScope buildRootScope() const
    {
        std::vector<NamedScopeEntry> entries;
        const auto& sqlFunctors = NESTypeCatalog::instance().getAllSqlFunctors();
        entries.reserve(sqlFunctors.size() + 8);
        for (const auto& namedFunctor : sqlFunctors)
        {
            entries.emplace_back(namedFunctor.getFullyQualifiedName(), namedFunctor.getUnderlyingFunctor());
        }
        if (modelCatalog != nullptr)
        {
            for (const auto& model : modelCatalog->getRegisteredModels())
            {
                const auto modelQid = QualifiedIdentifier::create(Identifier::parse(model.getName()));
                /// Catalog models override only if no built-in SQL functor owns the same identifier.
                const auto existing = std::ranges::find_if(
                    entries, [&modelQid](const NamedScopeEntry& entry) { return entry.getFullyQualifiedName() == modelQid; });
                if (existing == entries.end())
                {
                    entries.emplace_back(modelQid, makeRegisteredModelValue(model));
                }
            }
        }
        return NestedScope::create(Schema<NamedScopeEntry, Unordered>{std::move(entries)});
    }

    static NestedScope withUnaryWindowScope(const NestedScope& baseScope)
    {
        return baseScope.push(Schema<NamedScopeEntry, Unordered>{
            NamedScopeEntry{QualifiedIdentifier::parse("TUMBLING"), makeUnaryTumblingFunctor()},
            NamedScopeEntry{QualifiedIdentifier::parse("SLIDING"), makeUnarySlidingFunctor()}});
    }

    static NestedScope withBinaryWindowScope(const NestedScope& baseScope)
    {
        return baseScope.push(Schema<NamedScopeEntry, Unordered>{
            NamedScopeEntry{QualifiedIdentifier::parse("TUMBLING"), makeBinaryTumblingFunctor()},
            NamedScopeEntry{QualifiedIdentifier::parse("SLIDING"), makeBinarySlidingFunctor()}});
    }

    Identifier bindIdentifier(std::string identifier) const
    {
        auto identifierExpected = Identifier::tryParse(std::move(identifier));
        if (!identifierExpected.has_value())
        {
            throw std::move(identifierExpected).error();
        }
        return identifierExpected.value();
    }

    Identifier bindIdentifier(AntlrSQLParser::IdentifierContext* identifier) const
    {
        return bindIdentifier(identifier->getText());
    }

    QualifiedIdentifier bindQualifiedIdentifier(AntlrSQLParser::QualifiedIdentifierContext* qualifiedIdentifier) const
    {
        auto parsed = QualifiedIdentifier::tryParse(qualifiedIdentifier->getText());
        if (!parsed.has_value())
        {
            throw std::move(parsed).error();
        }
        return std::move(parsed).value();
    }

    std::string bindStringLiteral(AntlrSQLParser::StringLiteralContext* stringLiteral) const
    {
        return unescapeSqlString(stringLiteral->getText());
    }

    std::string bindStringLiteral(antlr4::Token* stringLiteral) const
    {
        PRECONDITION(
            stringLiteral->getType() == AntlrSQLParser::STRING_LITERAL, "Attempting to bind a non string token to a string literal");
        return unescapeSqlString(stringLiteral->getText());
    }

    bool bindBooleanLiteral(AntlrSQLParser::BoolLiteralContext* booleanLiteral) const
    {
        return booleanLiteral->TRUE() != nullptr;
    }

    Literal bindScalarLiteral(AntlrSQLParser::ScalarLiteralContext* literalAST) const
    {
        if (auto* const stringAST = literalAST->stringLiteral())
        {
            return bindStringLiteral(stringAST);
        }
        if (auto* const unsignedIntAST = literalAST->unsignedIntLiteral())
        {
            return from_chars_with_exception<uint64_t>(unsignedIntAST->getText());
        }
        if (auto* const floatAST = literalAST->floatLiteral())
        {
            return from_chars_with_exception<double>(floatAST->getText());
        }
        if (auto* const booleanAST = literalAST->boolLiteral())
        {
            return bindBooleanLiteral(booleanAST);
        }
        if (literalAST->null() != nullptr)
        {
            return std::monostate{};
        }
        INVARIANT(false, "Unknown scalar literal type");
        std::unreachable();
    }

    Literal bindConfigScalarLiteral(AntlrSQLParser::ConfigScalarLiteralContext* literalAST) const
    {
        if (auto* const stringAST = literalAST->stringLiteral())
        {
            return bindStringLiteral(stringAST);
        }
        if (auto* const signedIntAST = literalAST->signedConfigIntLiteral())
        {
            const auto digitsText = signedIntAST->UNSIGNED_INT_LITERAL()->getText();
            if (signedIntAST->sign != nullptr && signedIntAST->sign->getType() == AntlrSQLLexer::MINUS)
            {
                const auto fullText = "-" + digitsText;
                return from_chars_with_exception<int64_t>(fullText);
            }
            return from_chars_with_exception<uint64_t>(digitsText);
        }
        if (auto* const signedFloatAST = literalAST->signedConfigFloatLiteral())
        {
            const auto floatText = signedFloatAST->FLOAT_LITERAL()->getText();
            if (signedFloatAST->sign != nullptr && signedFloatAST->sign->getType() == AntlrSQLLexer::MINUS)
            {
                const auto fullText = "-" + floatText;
                return from_chars_with_exception<double>(fullText);
            }
            return from_chars_with_exception<double>(floatText);
        }
        if (auto* const booleanAST = literalAST->boolLiteral())
        {
            return bindBooleanLiteral(booleanAST);
        }
        if (literalAST->null() != nullptr)
        {
            return std::monostate{};
        }
        INVARIANT(false, "Unknown config scalar literal type");
        std::unreachable();
    }

    NESType bindNESType(AntlrSQLParser::TypeDefinitionContext* typeDefAST, const DataType::NULLABLE isNullable) const
    {
        if (auto* const builtinAST = dynamic_cast<AntlrSQLParser::BuiltinTypeDefinitionContext*>(typeDefAST))
        {
            bool translated = false;
            const auto normalized = normalizeBuiltinDataTypeName(builtinAST->DATA_TYPE()->getText(), &translated);
            const auto functorOpt = NESTypeCatalog::instance().findSqlFunctor(QualifiedIdentifier::create(Identifier::parse(normalized)));
            if (!functorOpt.has_value())
            {
                if (translated)
                {
                    throw UnknownDataType("{}, translated into {}", typeDefAST->getText(), normalized);
                }
                throw UnknownDataType("{}", typeDefAST->getText());
            }
            auto instantiated = functorOpt->instantiate(
                NESTypeParameters::fromValues({DataType::makeBooleanValue(isNullable == DataType::NULLABLE::IS_NULLABLE)}));
            if (!instantiated.has_value())
            {
                throw std::move(instantiated).error();
            }
            return *instantiated;
        }

        auto* const customAST = dynamic_cast<AntlrSQLParser::CustomTypeDefinitionContext*>(typeDefAST);
        INVARIANT(customAST != nullptr, "Unknown TypeDefinitionContext subclass");

        const auto typeName = bindIdentifier(customAST->identifier());
        const auto functorOpt = NESTypeCatalog::instance().findSqlFunctor(QualifiedIdentifier::create(typeName));
        if (!functorOpt.has_value())
        {
            throw UnknownDataType("{}", typeDefAST->getText());
        }

        std::vector<NESTypeParameters::Element> params;
        for (auto* const paramAST : customAST->typeParameter())
        {
            if (auto* const nestedTypeAST = paramAST->typeDefinition())
            {
                params.emplace_back(bindNESType(nestedTypeAST, DataType::NULLABLE::NOT_NULLABLE));
            }
            else if (auto* const cfgLitAST = paramAST->configLiteral())
            {
                if (auto* const scalarAST = cfgLitAST->configScalarLiteral())
                {
                    const auto lit = bindConfigScalarLiteral(scalarAST);
                    if (const auto* u = std::get_if<uint64_t>(&lit); u != nullptr && std::in_range<int64_t>(*u))
                    {
                        params.emplace_back(DataType::makeIntegerValue(static_cast<int64_t>(*u)));
                    }
                    else if (const auto* i = std::get_if<int64_t>(&lit))
                    {
                        params.emplace_back(DataType::makeIntegerValue(*i));
                    }
                    else if (const auto* b = std::get_if<bool>(&lit))
                    {
                        params.emplace_back(DataType::makeBooleanValue(*b));
                    }
                    else
                    {
                        params.emplace_back(makeScalarLiteralNESValue(lit));
                    }
                }
                else if (auto* const schemaAST = cfgLitAST->schemaCtor())
                {
                    params.emplace_back(makeSchemaNESValue(bindSchema(schemaAST->schemaDefinition())));
                }
            }
        }

        if (params.empty() && functorOpt->getUnderlyingFunctor().tryGetStructureAs<DataTypeFn>() != nullptr)
        {
            params.emplace_back(DataType::makeBooleanValue(isNullable == DataType::NULLABLE::IS_NULLABLE));
        }

        auto instantiated = functorOpt->instantiate(NESTypeParameters{std::move(params)});
        if (!instantiated.has_value())
        {
            throw std::move(instantiated).error();
        }
        return *instantiated;
    }

    DataType bindDataType(AntlrSQLParser::TypeDefinitionContext* typeDefAST, const DataType::NULLABLE isNullable) const
    {
        const auto nesType = bindNESType(typeDefAST, isNullable);
        if (const auto* trait = nesType.tryGetTrait<ScalarTypeTrait>())
        {
            if (const auto dtOpt = trait->tryGetDataType(nesType.getDescriptor()))
            {
                return *dtOpt;
            }
        }
        throw UnknownDataType("{}", typeDefAST->getText());
    }

    Schema<UnqualifiedUnboundField, Ordered> bindSchema(AntlrSQLParser::SchemaDefinitionContext* schemaDefAST) const
    {
        std::vector<UnqualifiedUnboundField> fields{};
        for (auto* const column : schemaDefAST->columnDefinition())
        {
            const auto isNullable
                = column->nullableDefinition() == nullptr ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
            auto dataType = bindDataType(column->typeDefinition(), isNullable);
            auto columnName = bindIdentifier(column->identifier());
            fields.emplace_back(columnName, dataType);
        }
        const auto boundSchema = Schema<UnqualifiedUnboundField, Ordered>::tryCreateCollisionFree(std::move(fields));
        if (!boundSchema.has_value())
        {
            throw FieldAlreadyExists(Schema<UnqualifiedUnboundField, Ordered>::createCollisionString(boundSchema.error()));
        }
        return boundSchema.value();
    }

    std::pair<Identifier, Literal> bindShowFilter(const AntlrSQLParser::ShowFilterContext* showFilterAST) const
    {
        return {bindIdentifier(showFilterAST->attr), bindConfigScalarLiteral(showFilterAST->value)};
    }

    std::pair<Identifier, Literal> bindDropFilter(const AntlrSQLParser::DropFilterContext* dropFilterAST) const
    {
        return {bindIdentifier(dropFilterAST->attr), bindConfigScalarLiteral(dropFilterAST->value)};
    }

    std::vector<AntlrSQLParser::NamedConfigLiteralContext*> getConfigOptions(AntlrSQLParser::OptionsClauseContext* optionsClause) const
    {
        if (optionsClause == nullptr || optionsClause->options == nullptr)
        {
            return {};
        }
        return optionsClause->options->namedConfigLiteral();
    }

    std::variant<Literal, Schema<UnqualifiedUnboundField, Ordered>>
    bindConfigValue(AntlrSQLParser::ConfigLiteralContext* literalAST) const
    {
        if (auto* const scalarAST = literalAST->configScalarLiteral())
        {
            return bindConfigScalarLiteral(scalarAST);
        }
        if (auto* const schemaCtorAST = literalAST->schemaCtor())
        {
            return bindSchema(schemaCtorAST->schemaDefinition());
        }
        INVARIANT(false, "Unknown config literal type");
        std::unreachable();
    }

    Schema<LiteralConfigValue, Ordered>
    bindConfigValues(const std::vector<AntlrSQLParser::NamedConfigLiteralContext*>& configOptions) const
    {
        std::vector<LiteralConfigValue> options{};
        for (auto* const configOption : configOptions)
        {
            auto configKey = bindQualifiedIdentifier(configOption->qualifiedIdentifier());
            auto value = bindConfigValue(configOption->configLiteral());
            ConfigLiteral literalValue{std::visit(
                Overloaded{
                    [](Literal simpleLiteralVariant) -> ConfigLiteral
                    {
                        return std::visit(
                            Overloaded{
                                [](const uint64_t unsignedInt)
                                {
                                    if (!std::in_range<int64_t>(unsignedInt))
                                    {
                                        throw InvalidQuerySyntax(
                                            "Integer literal {} exceeds the supported signed integer range", unsignedInt);
                                    }
                                    return ConfigLiteral{static_cast<int64_t>(unsignedInt)};
                                },
                                [](auto literal) -> ConfigLiteral { return ConfigLiteral{std::move(literal)}; }},
                            std::move(simpleLiteralVariant));
                    },
                    [](Schema<UnqualifiedUnboundField, Ordered> schema) -> ConfigLiteral { return ConfigLiteral{std::move(schema)}; }},
                std::move(value))};
            options.emplace_back(std::move(configKey), std::move(literalValue));
        }
        return Schema<LiteralConfigValue, Ordered>{std::move(options)};
    }

    ConfigMap bindConfigOptions(const std::vector<AntlrSQLParser::NamedConfigLiteralContext*>& configOptions) const
    {
        ConfigMap boundConfigOptions{};
        for (auto* const configOption : configOptions)
        {
            const auto qid = bindQualifiedIdentifier(configOption->qualifiedIdentifier());
            if (qid.size() != 2)
            {
                throw InvalidConfigParameter(
                    "Config key needs to be qualified exactly once, but was {}", configOption->qualifiedIdentifier()->getText());
            }
            auto it = qid.begin();
            const auto rootIdentifier = *it;
            ++it;
            const auto optionName = *it;
            boundConfigOptions.try_emplace(
                rootIdentifier, std::unordered_map<Identifier, std::variant<Literal, Schema<UnqualifiedUnboundField, Ordered>>>{});

            if (!boundConfigOptions.at(rootIdentifier).try_emplace(optionName, bindConfigValue(configOption->configLiteral())).second)
            {
                throw InvalidConfigParameter("Duplicate option: {}", configOption->qualifiedIdentifier()->getText());
            }
        }
        return boundConfigOptions;
    }

    ConfigMultiMap bindConfigOptionsWithDuplicates(const std::vector<AntlrSQLParser::NamedConfigLiteralContext*>& configOptions) const
    {
        ConfigMultiMap boundConfigOptions;
        for (auto* const configOption : configOptions)
        {
            boundConfigOptions.emplace_back(
                bindQualifiedIdentifier(configOption->qualifiedIdentifier()), bindConfigValue(configOption->configLiteral()));
        }
        return boundConfigOptions;
    }

    std::tuple<
        GeneralSourceConfig,
        PluginSourceConfiguration,
        InputFormatterDescriptor,
        std::optional<Schema<UnqualifiedUnboundField, Ordered>>>
    bindSourceConfig(
        const Identifier& sourceType,
        const std::vector<AntlrSQLParser::NamedConfigLiteralContext*>& configOptionsAst,
        const Schema<ConfigFieldDefault, Ordered>& configDefaults,
        const Schema<ConfigFieldTransformation, Unordered>& configTransformations) const
    {
        const auto configOptions = bindConfigValues(configOptionsAst);

        const auto inputFormatterType = [&]
        {
            auto expected
                = optionalToExpected(
                      configOptions.getFieldByName(QualifiedIdentifier::parse("INPUT_FORMATTER.TYPE"))
                          .or_else(
                              [&]
                              {
                                  return configDefaults.getFieldByName(QualifiedIdentifier::parse("INPUT_FORMATTER.TYPE"))
                                      .transform(
                                          [](const ConfigFieldDefault& defaultInputFormatter)
                                          {
                                              return LiteralConfigValue{
                                                  defaultInputFormatter.getFullyQualifiedName(), defaultInputFormatter.get()};
                                          });
                              }),
                      [] { return InvalidQuerySyntax("INPUT_FORMATTER.TYPE must be specified"); })
                      .and_then(
                          [](const LiteralConfigValue& value)
                          { return tryGetOr<std::string>(value.getValue(), expectedType<std::string>()).and_then(Identifier::tryParse); });
            return unwrapOrThrow(std::move(expected));
        }();

        auto configSchema
            = SourceCatalog::getConfigSchema(sourceType, inputFormatterType)
                  .transform([&](const SourceConfigSchema& schema)
                             { return schema.withConfigDefaults(configDefaults).withConfigTransformations(configTransformations); });

        return unwrapOrThrow(
            configSchema.and_then([&](const SourceConfigSchema& schema) { return schema.resolveConfigs(configOptions); }));
    }

    std::tuple<GeneralSinkConfig, AnonymousSinkSchema, PluginSinkConfiguration, OutputFormatterDescriptor> bindSinkConfig(
        const Identifier& sinkType,
        const std::vector<AntlrSQLParser::NamedConfigLiteralContext*>& configOptionsAst,
        const Schema<ConfigFieldDefault, Ordered>& configDefaults,
        const Schema<ConfigFieldTransformation, Unordered>& configTransformations) const
    {
        const auto configOptions = bindConfigValues(configOptionsAst);

        const auto outputFormatterType = [&]
        {
            const auto typeLiteral
                = configOptions.getFieldByName(QualifiedIdentifier::parse("OUTPUT_FORMATTER.TYPE"))
                      .or_else(
                          [&]
                          {
                              return configDefaults.getFieldByName(QualifiedIdentifier::parse("OUTPUT_FORMATTER.TYPE"))
                                  .transform(
                                      [](const ConfigFieldDefault& defaultFormatter)
                                      { return LiteralConfigValue{defaultFormatter.getFullyQualifiedName(), defaultFormatter.get()}; });
                          });
            if (!typeLiteral.has_value())
            {
                return Identifier::parse("NATIVE");
            }
            return unwrapOrThrow(
                tryGetOr<std::string>(typeLiteral->getValue(), expectedType<std::string>()).and_then(Identifier::tryParse));
        }();
        auto configSchema
            = SinkCatalog::getConfigSchema(sinkType, outputFormatterType)
                  .transform([&](const SinkConfigSchema& schema)
                             { return schema.withConfigDefaults(configDefaults).withConfigTransformations(configTransformations); });
        return unwrapOrThrow(configSchema.and_then([&](const SinkConfigSchema& schema) { return schema.resolveConfigs(configOptions); }));
    }

    StatementOutputFormat bindFormat(const Identifier& formatAST) const
    {
        if (formatAST == Identifier::parse("TEXT"))
        {
            return StatementOutputFormat::TEXT;
        }
        if (formatAST == Identifier::parse("JSON"))
        {
            return StatementOutputFormat::JSON;
        }
        throw InvalidQuerySyntax("Unknown format {}", formatAST);
    }

    CreateLogicalSourceStatement
    bindCreateLogicalSourceStatement(AntlrSQLParser::CreateLogicalSourceDefinitionContext* logicalSourceDefAST) const
    {
        const auto sourceName = bindIdentifier(logicalSourceDefAST->sourceName);
        const auto schema = bindSchema(logicalSourceDefAST->schemaDefinition());
        return CreateLogicalSourceStatement{.name = sourceName, .schema = schema};
    }

    CreatePhysicalSourceStatement
    bindCreatePhysicalSourceStatement(AntlrSQLParser::CreatePhysicalSourceDefinitionContext* physicalSourceDefAST) const
    {
        auto logicalSourceName = LogicalSourceName(bindIdentifier(physicalSourceDefAST->logicalSource));
        const Identifier sourceType = bindIdentifier(physicalSourceDefAST->type);
        const auto validatedConfig = bindSourceConfig(
            sourceType, getConfigOptions(physicalSourceDefAST->optionsClause()), defaultConfigValues, configTransformations);

        const auto& [generalConfig, pluginConfig, inputFormatter, schemaOpt] = validatedConfig;
        if (schemaOpt.has_value())
        {
            throw InvalidQuerySyntax("Schema cannot be specified for physical source attached to logical source");
        }
        return CreatePhysicalSourceStatement{
            .logicalSourceName = std::move(logicalSourceName),
            .generalSourceConfig = generalConfig,
            .pluginSourceConfig = pluginConfig,
            .pluginInputFormatterConfig = inputFormatter};
    }

    CreateWorkerStatement bindCreateWorkerStatement(AntlrSQLParser::CreateWorkerDefinitionContext* workerDefAST) const
    {
        auto configs = bindConfigOptionsWithDuplicates(getConfigOptions(workerDefAST->optionsClause()));

        const auto maxOperators = [&] -> std::optional<size_t>
        {
            auto it = std::ranges::find_if(
                configs,
                [](const auto& key)
                { return key.first.size() == 1 && *std::ranges::begin(key.first) == Identifier::parse("MAX_OPERATORS"); });
            if (it != configs.end())
            {
                const auto* literalOpt = std::get_if<Literal>(&it->second);
                if (literalOpt != nullptr && std::holds_alternative<uint64_t>(*literalOpt))
                {
                    return static_cast<size_t>(std::get<uint64_t>(*literalOpt));
                }
                throw InvalidQuerySyntax("MAX_OPERATORS must be an unsigned integer literal");
            }
            return std::nullopt;
        }();

        auto dataAddress = [&] -> std::string
        {
            auto it = std::ranges::find_if(
                configs,
                [](const auto& key) { return key.first.size() == 1 && *std::ranges::begin(key.first) == Identifier::parse("DATA"); });
            if (it != configs.end())
            {
                const Literal* literalOpt = std::get_if<Literal>(&it->second);
                if (literalOpt != nullptr && std::holds_alternative<std::string>(*literalOpt))
                {
                    return URI(std::get<std::string>(*literalOpt)).toString();
                }
                throw InvalidQuerySyntax("DATA must be a string literal");
            }
            return {};
        }();

        auto downStreams = [&] -> std::vector<std::string>
        {
            return configs
                | std::views::filter(
                       [](const auto& option)
                       { return option.first.size() == 1 && *std::ranges::begin(option.first) == Identifier::parse("DOWNSTREAM"); })
                | std::views::values
                | std::views::transform(
                       [](const auto& value)
                       {
                           const Literal* literalOpt = std::get_if<Literal>(&value);
                           if (literalOpt != nullptr && std::holds_alternative<std::string>(*literalOpt))
                           {
                               return URI(std::get<std::string>(*literalOpt)).toString();
                           }
                           throw InvalidQuerySyntax("DOWNSTREAM must be a string literal");
                       })
                | std::ranges::to<std::vector<std::string>>();
        }();

        std::unordered_map<std::string, std::string> workerConfigMap;
        for (const auto& [key, val] : configs)
        {
            if (key.size() > 1)
            {
                if (const auto* litOpt = std::get_if<Literal>(&val))
                {
                    const auto dotted = fmt::format(
                        "{}", fmt::join(key | std::views::transform([](const Identifier& id) { return toLowerCase(id.asCanonicalString()); }), "."));
                    workerConfigMap.emplace(dotted, literalToString(*litOpt));
                }
            }
        }

        return CreateWorkerStatement{
            .host = URI(bindStringLiteral(workerDefAST->hostaddr)).toString(),
            .dataAddress = std::move(dataAddress),
            .maxOperators = maxOperators,
            .downstream = std::move(downStreams),
            .config = std::move(workerConfigMap)};
    }

    CreateSinkStatement bindCreateSinkStatement(AntlrSQLParser::CreateSinkDefinitionContext* sinkDefAST) const
    {
        const auto sinkName = bindIdentifier(sinkDefAST->sinkName);
        const Identifier sinkType = bindIdentifier(sinkDefAST->type);
        auto [generalConfig, anonymousSinkSchema, pluginConfig, outputFormatterDescriptor]
            = bindSinkConfig(sinkType, getConfigOptions(sinkDefAST->optionsClause()), defaultConfigValues, configTransformations);

        if (!std::holds_alternative<std::monostate>(anonymousSinkSchema))
        {
            throw InvalidConfigParameter("Cannot set sink schema as parameter in named sink {}", sinkName);
        }

        const auto schema = bindSchema(sinkDefAST->schemaDefinition());
        return CreateSinkStatement{
            .name = sinkName,
            .schema = schema,
            .generalSinkConfig = std::move(generalConfig),
            .pluginSinkConfig = std::move(pluginConfig),
            .outputFormatterDescriptor = std::move(outputFormatterDescriptor)};
    }

    CreateModelStatement bindCreateModelStatement(AntlrSQLParser::CreateModelDefinitionContext* modelDefAST) const
    {
        const auto modelName = bindIdentifier(modelDefAST->modelName);
        const auto modelPath = bindStringLiteral(modelDefAST->modelPath);
        auto inputs = normalizeNonNullableModelFields(bindSchema(modelDefAST->inputSchema));
        auto outputs = normalizeNonNullableModelFields(bindSchema(modelDefAST->outputSchema));
        return CreateModelStatement{
            .name = fmt::format("{}", modelName), .path = modelPath, .inputs = std::move(inputs), .outputs = std::move(outputs)};
    }

    Statement bindCreateStatement(AntlrSQLParser::CreateStatementContext* createAST) const
    {
        if (auto* const logicalSourceDefAST = createAST->createDefinition()->createLogicalSourceDefinition();
            logicalSourceDefAST != nullptr)
        {
            return bindCreateLogicalSourceStatement(logicalSourceDefAST);
        }
        if (auto* const physicalSourceDefAST = createAST->createDefinition()->createPhysicalSourceDefinition();
            physicalSourceDefAST != nullptr)
        {
            return bindCreatePhysicalSourceStatement(physicalSourceDefAST);
        }
        if (auto* const sinkDefAST = createAST->createDefinition()->createSinkDefinition(); sinkDefAST != nullptr)
        {
            return bindCreateSinkStatement(sinkDefAST);
        }
        if (auto* const workerDefAST = createAST->createDefinition()->createWorkerDefinition(); workerDefAST != nullptr)
        {
            return bindCreateWorkerStatement(workerDefAST);
        }
        if (auto* const modelDefAST = createAST->createDefinition()->createModelDefinition(); modelDefAST != nullptr)
        {
            return bindCreateModelStatement(modelDefAST);
        }
        if (auto* const catalogItemDefAST = createAST->createDefinition()->createCatalogItemDefinition(); catalogItemDefAST != nullptr)
        {
            throw InvalidStatement("CREATE catalog item statements are not implemented yet");
        }
        throw InvalidStatement("Unrecognized CREATE statement");
    }

    ShowLogicalSourcesStatement bindShowLogicalSourcesStatement(
        const AntlrSQLParser::ShowFilterContext* showFilter, const std::optional<StatementOutputFormat>& format) const
    {
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            static const auto NameIdentifier = Identifier::parse("NAME");
            if (attr != NameIdentifier)
            {
                throw InvalidQuerySyntax("Filter for SHOW LOGICAL SOURCES must be on name attribute");
            }
            if (!std::holds_alternative<std::string>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW LOGICAL SOURCES must be a string");
            }
            return ShowLogicalSourcesStatement{.name = bindIdentifier(std::get<std::string>(value)), .format = format};
        }
        return ShowLogicalSourcesStatement{.name = std::nullopt, .format = format};
    }

    ShowPhysicalSourcesStatement bindShowPhysicalSourcesStatement(
        const AntlrSQLParser::ShowFilterContext* showFilter,
        const AntlrSQLParser::ShowPhysicalSourcesSubjectContext* subjectAST,
        const std::optional<StatementOutputFormat>& format) const
    {
        std::optional<LogicalSourceName> logicalSource;
        if (subjectAST->logicalSourceName != nullptr)
        {
            logicalSource = LogicalSourceName{bindIdentifier(subjectAST->logicalSourceName)};
        }

        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            static const auto IdIdentifier = Identifier::parse("ID");
            if (attr != IdIdentifier)
            {
                throw InvalidQuerySyntax("Filter for SHOW PHYSICAL SOURCES must be on id attribute");
            }
            if (!std::holds_alternative<uint64_t>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW PHYSICAL SOURCES must be an unsigned integer");
            }
            return ShowPhysicalSourcesStatement{
                .logicalSource = std::move(logicalSource), .id = static_cast<uint32_t>(std::get<uint64_t>(value)), .format = format};
        }
        return ShowPhysicalSourcesStatement{.logicalSource = std::move(logicalSource), .id = std::nullopt, .format = format};
    }

    ShowSinksStatement
    bindShowSinksStatement(const AntlrSQLParser::ShowFilterContext* showFilter, const std::optional<StatementOutputFormat>& format) const
    {
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            static const auto NameIdentifier = Identifier::parse("NAME");
            if (attr != NameIdentifier)
            {
                throw InvalidQuerySyntax("Filter for SHOW SINKS must be on name attribute");
            }
            if (!std::holds_alternative<std::string>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW SINKS must be a string");
            }
            return ShowSinksStatement{.name = bindIdentifier(std::get<std::string>(value)), .format = format};
        }
        return ShowSinksStatement{.name = std::nullopt, .format = format};
    }

    ShowQueriesStatement
    bindShowQueriesStatement(const AntlrSQLParser::ShowFilterContext* showFilter, const std::optional<StatementOutputFormat>& format) const
    {
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            static const auto IdIdentifier = Identifier::parse("ID");
            if (attr != IdIdentifier)
            {
                throw InvalidQuerySyntax("Filter for SHOW QUERIES must be on id attribute");
            }
            if (!std::holds_alternative<std::string>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW QUERIES must be a string");
            }
            return ShowQueriesStatement{.id = DistributedQueryId{std::get<std::string>(value)}, .format = format};
        }
        return ShowQueriesStatement{.id = std::nullopt, .format = format};
    }

    Statement bindShowStatement(AntlrSQLParser::ShowStatementContext* showAST) const
    {
        auto* const showFilter = showAST->showFilter();
        const std::optional<StatementOutputFormat> format
            = showAST->showFormat != nullptr ? std::make_optional(bindFormat(bindIdentifier(showAST->showFormat))) : std::nullopt;
        auto* const subject = showAST->showSubject();

        if (dynamic_cast<AntlrSQLParser::ShowLogicalSourcesSubjectContext*>(subject) != nullptr)
        {
            return bindShowLogicalSourcesStatement(showFilter, format);
        }
        if (const auto* physSubject = dynamic_cast<AntlrSQLParser::ShowPhysicalSourcesSubjectContext*>(subject))
        {
            return bindShowPhysicalSourcesStatement(showFilter, physSubject, format);
        }
        if (dynamic_cast<AntlrSQLParser::ShowQueriesSubjectContext*>(subject) != nullptr)
        {
            return bindShowQueriesStatement(showFilter, format);
        }
        if (dynamic_cast<AntlrSQLParser::ShowSinksSubjectContext*>(subject) != nullptr)
        {
            return bindShowSinksStatement(showFilter, format);
        }
        if (dynamic_cast<AntlrSQLParser::ShowModelsSubjectContext*>(subject) != nullptr)
        {
            if (showFilter != nullptr)
            {
                throw InvalidQuerySyntax("SHOW MODELS does not support a filter");
            }
            return ShowModelsStatement{.format = format};
        }
        if (dynamic_cast<AntlrSQLParser::ShowVersionSubjectContext*>(subject) != nullptr)
        {
            if (showFilter != nullptr)
            {
                throw InvalidQuerySyntax("SHOW VERSION does not support a filter");
            }
            return ShowVersionStatement{.format = format};
        }
        if (dynamic_cast<AntlrSQLParser::ShowWorkerStatusSubjectContext*>(subject) != nullptr)
        {
            if (showFilter != nullptr)
            {
                const auto [attr, value] = bindShowFilter(showFilter);
                if (attr != Identifier::parse("HOST") || !std::holds_alternative<std::string>(value))
                {
                    throw InvalidQuerySyntax("Filter for SHOW WORKER STATUS must be HOST = '<host>'");
                }
                return WorkerStatusStatement{.host = {std::get<std::string>(value)}};
            }
            return WorkerStatusStatement{.host = {}};
        }
        throw InvalidStatement("Unrecognized SHOW statement");
    }

    template <typename T>
    T requireFilterValue(
        const std::pair<Identifier, Literal>& filter,
        const std::string_view expectedAttr,
        const std::string_view expectedTypeDescription,
        const std::string_view subject) const
    {
        if (filter.first != bindIdentifier(std::string{expectedAttr}))
        {
            throw InvalidQuerySyntax("Filter for {} must be on {} attribute", subject, expectedAttr);
        }
        if (!std::holds_alternative<T>(filter.second))
        {
            throw InvalidQuerySyntax("Filter value for {} must be {}", subject, expectedTypeDescription);
        }
        return std::get<T>(filter.second);
    }

    DropLogicalSourceStatement bindDropLogicalSource(const std::pair<Identifier, Literal>& filter) const
    {
        return DropLogicalSourceStatement{
            LogicalSourceName(Identifier::parse(requireFilterValue<std::string>(filter, "NAME", "a string", "DROP LOGICAL SOURCE")))};
    }

    [[nodiscard]] DropPhysicalSourceStatement bindDropPhysicalSource(const std::pair<Identifier, Literal>& filter) const
    {
        const auto id = requireFilterValue<uint64_t>(filter, "ID", "an unsigned integer", "DROP PHYSICAL SOURCE");
        if (sourceCatalog != nullptr)
        {
            if (const auto physicalSource = sourceCatalog->getPhysicalSource(PhysicalSourceId{id}))
            {
                return DropPhysicalSourceStatement{*physicalSource};
            }
        }
        throw UnknownSourceName("There is no physical source with id {}", id);
    }

    DropQueryStatement bindDropQuery(const std::pair<Identifier, Literal>& filter) const
    {
        return DropQueryStatement{.id = DistributedQueryId(requireFilterValue<std::string>(filter, "ID", "a string", "DROP QUERY"))};
    }

    DropSinkStatement bindDropSink(const std::pair<Identifier, Literal>& filter) const
    {
        return DropSinkStatement{Identifier::parse(requireFilterValue<std::string>(filter, "NAME", "a string", "DROP SINK"))};
    }

    DropModelStatement bindDropModel(const std::pair<Identifier, Literal>& filter) const
    {
        return DropModelStatement{.name = requireFilterValue<std::string>(filter, "NAME", "a string", "DROP MODEL")};
    }

    DropWorkerStatement bindDropWorker(const std::pair<Identifier, Literal>& filter) const
    {
        return DropWorkerStatement{.host = URI(requireFilterValue<std::string>(filter, "HOST", "a string", "DROP WORKER")).toString()};
    }

    Statement bindDropStatement(AntlrSQLParser::DropStatementContext* dropAst) const
    {
        const auto* const dropFilter = dropAst->dropFilter();
        PRECONDITION(dropFilter != nullptr, "Drop statement must have a WHERE filter");
        const auto filter = bindDropFilter(dropFilter);
        auto* const subject = dropAst->dropSubject();

        if (auto* const dropSourceAst = subject->dropSource())
        {
            if (dropSourceAst->dropLogicalSourceSubject() != nullptr)
            {
                return bindDropLogicalSource(filter);
            }
            if (dropSourceAst->dropPhysicalSourceSubject() != nullptr)
            {
                return bindDropPhysicalSource(filter);
            }
        }
        if (subject->dropQuery() != nullptr)
        {
            return bindDropQuery(filter);
        }
        if (subject->dropSink() != nullptr)
        {
            return bindDropSink(filter);
        }
        if (subject->dropModel() != nullptr)
        {
            return bindDropModel(filter);
        }
        if (subject->dropWorker() != nullptr)
        {
            return bindDropWorker(filter);
        }
        throw InvalidStatement("Unrecognized DROP statement");
    }

    std::unordered_set<ExplainStage> bindExplainStages(AntlrSQLParser::ExplainStagesContext* stagesContext) const
    {
        std::unordered_set<ExplainStage> explainStages;
        for (auto* const stageContext : stagesContext->explainStage())
        {
            const auto identifier = bindIdentifier(stageContext->identifier());
            if (identifier == Identifier::parse("logical"))
            {
                if (!explainStages.insert(ExplainStage::Logical).second)
                {
                    throw InvalidStatement("Duplicate EXPLAIN stage: LOGICAL");
                }
            }
            else if (identifier == Identifier::parse("optimized"))
            {
                if (!explainStages.insert(ExplainStage::Optimized).second)
                {
                    throw InvalidStatement("Duplicate EXPLAIN stage: OPTIMIZED");
                }
            }
            else if (identifier == Identifier::parse("distributed"))
            {
                if (!explainStages.insert(ExplainStage::Distributed).second)
                {
                    throw InvalidStatement("Duplicate EXPLAIN stage: DISTRIBUTED");
                }
            }
            else
            {
                throw InvalidStatement("Unrecognized EXPLAIN stage: {}", identifier);
            }
        }
        return explainStages;
    }

    ExplainFormat bindExplainFormat(AntlrSQLParser::ExplainFormatContext* formatAST) const
    {
        const auto parsedFormatIdentifier = bindIdentifier(formatAST->identifier());
        if (parsedFormatIdentifier == Identifier::parse("text"))
        {
            return ExplainFormat::Text;
        }
        if (parsedFormatIdentifier == Identifier::parse("verbose"))
        {
            return ExplainFormat::Verbose;
        }
        if (parsedFormatIdentifier == Identifier::parse("visual"))
        {
            return ExplainFormat::Visual;
        }
        throw InvalidStatement("Unrecognized EXPLAIN format: {}", parsedFormatIdentifier);
    }

    Statement bindExplainStatement(AntlrSQLParser::ExplainStatementContext* explainAst) const
    {
        INVARIANT(explainAst->queryWithSink() != nullptr, "Should be enforced by antlr");
        ExplainFormat format = ExplainFormat::Visual;
        std::unordered_set<ExplainStage> stages = {ExplainStage::Logical, ExplainStage::Optimized, ExplainStage::Distributed};
        if (auto* const stagesAST = explainAst->explainStages())
        {
            stages = bindExplainStages(stagesAST);
        }
        if (auto* const formatAST = explainAst->explainFormat())
        {
            format = bindExplainFormat(formatAST);
        }
        const auto rootScope = buildRootScope();
        return ExplainQueryStatement{
            .plan = bindQueryWithSink(explainAst->queryWithSink(), rootScope),
            .explainFormat = format,
            .explainStages = std::move(stages)};
    }

    /// --- Recursive-Descent Query & Expression Binder ---

    Windowing::TimeMeasure bindIntervalUnit(AntlrSQLParser::DatetimeFieldContext* unitAST, const uint64_t amount) const
    {
        if (unitAST->MILLISECOND() != nullptr || unitAST->MILLISECONDS() != nullptr || unitAST->MS() != nullptr)
        {
            return API::Milliseconds(amount);
        }
        if (unitAST->SECOND() != nullptr || unitAST->SECONDS() != nullptr || unitAST->SEC() != nullptr)
        {
            return API::Seconds(amount);
        }
        if (unitAST->MINUTE() != nullptr || unitAST->MINUTES() != nullptr)
        {
            return API::Minutes(amount);
        }
        if (unitAST->HOUR() != nullptr || unitAST->HOURS() != nullptr)
        {
            return API::Hours(amount);
        }
        if (unitAST->DAY() != nullptr || unitAST->DAYS() != nullptr)
        {
            return API::Days(amount);
        }
        throw NotImplemented("Calendar interval unit '{}' is not supported for window durations", unitAST->getText());
    }

    TimeInterval bindIntervalExpression(
        AntlrSQLParser::IntervalExpressionContext* intervalAST,
        const NestedScope& scope,
        QueryBindState* queryState,
        const ScalarClauseMode clauseMode) const
    {
        auto* const valAST = intervalAST->value;
        uint64_t amount = 0;
        if (valAST->UNSIGNED_INT_LITERAL() != nullptr)
        {
            if (valAST->sign != nullptr && valAST->sign->getType() == AntlrSQLLexer::MINUS)
            {
                throw InvalidQuerySyntax("Window interval duration must be non-negative, got {}", intervalAST->getText());
            }
            amount = from_chars_with_exception<uint64_t>(valAST->UNSIGNED_INT_LITERAL()->getText());
        }
        else if (valAST->FLOAT_LITERAL() != nullptr)
        {
            throw InvalidQuerySyntax("Window interval duration must be an integer, got {}", intervalAST->getText());
        }
        else if (auto* const nestedScalar = valAST->openScalarExpression())
        {
            const auto fn = bindOpenScalarExpression(nestedScalar, scope, queryState, clauseMode);
            if (const auto constFn = fn.tryGetAs<ConstantValueLogicalFunction>())
            {
                const auto parsed = from_chars<uint64_t>(constFn->get().getConstantValue());
                if (!parsed.has_value())
                {
                    throw InvalidQuerySyntax("Window interval duration must be a non-negative integer, got {}", intervalAST->getText());
                }
                amount = *parsed;
            }
            else
            {
                throw NotImplemented("Creating an interval expression from a non-literal expression {}", intervalAST->getText());
            }
        }
        return TimeInterval{.measure = bindIntervalUnit(intervalAST->unit, amount)};
    }

    std::string extractTypedConstantArgument(AntlrSQLParser::OpenScalarExpressionContext* exprAST, bool negated = false) const
    {
        if (auto* const parenAST = dynamic_cast<AntlrSQLParser::ParenthesizedScalarExpressionContext*>(exprAST))
        {
            return extractTypedConstantArgument(parenAST->openScalarExpression(), negated);
        }
        if (auto* const unaryAST = dynamic_cast<AntlrSQLParser::UnaryArithmeticExpressionContext*>(exprAST))
        {
            const bool nextNegated = (unaryAST->op->getType() == AntlrSQLLexer::MINUS) ? !negated : negated;
            return extractTypedConstantArgument(unaryAST->openScalarExpression(), nextNegated);
        }
        if (auto* const litAST = dynamic_cast<AntlrSQLParser::LiteralScalarExpressionContext*>(exprAST))
        {
            if (auto* const scalarLit = litAST->literal()->scalarLiteral())
            {
                if (auto* const strAST = scalarLit->stringLiteral())
                {
                    if (negated)
                    {
                        throw InvalidQuerySyntax("Cannot negate string literal in typed constant: {}", exprAST->getText());
                    }
                    if (strAST->getText().size() <= 2)
                    {
                        throw InvalidQuerySyntax(
                            "A constant string literal must contain at least two quotes and must not be empty at {}",
                            exprAST->getText());
                    }
                    return bindStringLiteral(strAST);
                }
                if (auto* const uintAST = scalarLit->unsignedIntLiteral())
                {
                    return negated ? ("-" + uintAST->getText()) : uintAST->getText();
                }
                if (auto* const fltAST = scalarLit->floatLiteral())
                {
                    return negated ? ("-" + fltAST->getText()) : fltAST->getText();
                }
                if (auto* const boolAST = scalarLit->boolLiteral())
                {
                    if (negated)
                    {
                        throw InvalidQuerySyntax("Cannot negate boolean literal in typed constant: {}", exprAST->getText());
                    }
                    return boolAST->getText();
                }
            }
        }
        throw InvalidQuerySyntax("Expected constant literal inside data type constructor, got {}", exprAST->getText());
    }

    NESValue bindFunctionArgumentValue(
        AntlrSQLParser::FunctionCallArgumentContext* argAST,
        const NestedScope& scope,
        QueryBindState* queryState,
        const ScalarClauseMode clauseMode) const
    {
        if (auto* const scalarArg = dynamic_cast<AntlrSQLParser::ScalarFunctionArgContext*>(argAST))
        {
            return bindScalarExpressionToValue(scalarArg->openScalarExpression(), scope, queryState, clauseMode);
        }
        if (auto* const subqueryArg = dynamic_cast<AntlrSQLParser::SubqueryFunctionArgContext*>(argAST))
        {
            auto subPlan = bindUnionSeq(subqueryArg->unionSeq(), scope);
            return makeLogicalOperatorValue(subPlan.getRootOperators().at(0));
        }
        if (auto* const joinArg = dynamic_cast<AntlrSQLParser::JoinFunctionArgContext*>(argAST))
        {
            auto joinPlan = bindJoinRelationalExpression(joinArg->joinRelationalExpression(), scope);
            return makeLogicalOperatorValue(joinPlan.getRootOperators().at(0));
        }
        if (auto* const dtArg = dynamic_cast<AntlrSQLParser::DataTypeFunctionArgContext*>(argAST))
        {
            throw InvalidQuerySyntax("Unexpected bare data type '{}' in value argument position", dtArg->getText());
        }
        INVARIANT(false, "Unhandled FunctionCallArgumentContext");
        std::unreachable();
    }

    NESType resolveCalleeToInstantiatedType(
        AntlrSQLParser::FunctionCallContext* calleeAST,
        const NestedScope& scope,
        const std::vector<NESValue>& positionalValues,
        const Schema<NamedNESValue, Ordered>* namedValues) const
    {
        auto* const refAST = dynamic_cast<AntlrSQLParser::FunctionReferenceContext*>(calleeAST);
        if (refAST == nullptr)
        {
            throw InvalidQuerySyntax("Unsupported chained function call target '{}'", calleeAST->getText());
        }

        const auto fnId = bindIdentifier(refAST->identifier());
        const auto fnQid = QualifiedIdentifier::create(fnId);
        const auto foundOpt = scope.find(fnQid);
        if (!foundOpt.has_value() || !std::holds_alternative<NESFunctor>(*foundOpt))
        {
            auto registeredNamesAggregation
                = AggregationLogicalFunctionRegistry::instance().getRegisteredNames() | std::ranges::to<std::vector>();
            std::ranges::sort(registeredNamesAggregation);
            throw InvalidQuerySyntax(
                "Unknown function: {}. Supported aggregation functions are {}.",
                toUpperCase(fnId.getOriginalString()),
                fmt::join(registeredNamesAggregation, ", "));
        }

        const auto& functor = std::get<NESFunctor>(*foundOpt);
        if (const auto& expectedArgs = functor.getExpectedArguments(); expectedArgs.has_value() && !expectedArgs->empty())
        {
            if (namedValues != nullptr)
            {
                std::vector<NamedNESTypeParameter> namedTypeParams;
                namedTypeParams.reserve(namedValues->size());
                for (const auto& nv : *namedValues)
                {
                    const auto paramSpec = expectedArgs->getParameters().getFieldByName(nv.getFullyQualifiedName());
                    if (paramSpec.has_value() && paramSpec->kind == ExpectedNESTypeParameter::Kind::ValueParameter)
                    {
                        namedTypeParams.emplace_back(nv.getFullyQualifiedName(), nv.getValue());
                    }
                    else
                    {
                        namedTypeParams.emplace_back(nv.getFullyQualifiedName(), nv.getValue().getType());
                    }
                }
                auto instExp = functor.instantiate(NESTypeParameters::named(Schema<NamedNESTypeParameter, Ordered>{std::move(namedTypeParams)}));
                if (!instExp.has_value())
                {
                    throw std::move(instExp).error();
                }
                return *instExp;
            }

            std::vector<NESTypeParameters::Element> posTypeParams;
            posTypeParams.reserve(positionalValues.size());
            for (size_t i = 0; i < positionalValues.size(); ++i)
            {
                const auto paramSpec = expectedArgs->getParameters()[i];
                if (paramSpec.has_value() && paramSpec->kind == ExpectedNESTypeParameter::Kind::ValueParameter)
                {
                    posTypeParams.emplace_back(positionalValues[i]);
                }
                else
                {
                    posTypeParams.emplace_back(positionalValues[i].getType());
                }
            }
            auto instExp = functor.instantiate(NESTypeParameters{std::move(posTypeParams)});
            if (!instExp.has_value())
            {
                throw std::move(instExp).error();
            }
            return *instExp;
        }

        auto instExp = functor.instantiate(NESTypeParameters{});
        if (!instExp.has_value())
        {
            throw std::move(instExp).error();
        }
        return *instExp;
    }

    NESValue finalizeConstructedCallValue(
        NESValue constructed,
        QueryBindState* queryState,
        const ScalarClauseMode clauseMode,
        const std::string_view callText) const
    {
        const auto* boundAgg = constructed.tryGetAs<BoundAggregationCall>();
        if (boundAgg == nullptr)
        {
            return constructed;
        }

        if (queryState == nullptr
            || (clauseMode != ScalarClauseMode::SelectTopLevel && clauseMode != ScalarClauseMode::SelectNested
                && clauseMode != ScalarClauseMode::Having))
        {
            throw InvalidQuerySyntax("Aggregation function '{}' is not allowed in this clause: {}", boundAgg->aggregationName, callText);
        }

        const auto onField = [&]() -> TypedLogicalFunction<UnboundFieldAccessLogicalFunction>
        {
            if (std::holds_alternative<AsteriskTag>(boundAgg->argument))
            {
                return UnboundFieldAccessLogicalFunction{Identifier::parse("*")};
            }
            const auto& argFn = std::get<LogicalFunction>(boundAgg->argument);
            if (const auto directField = argFn.tryGetAs<UnboundFieldAccessLogicalFunction>())
            {
                return *directField;
            }
            const auto tempName = bindIdentifier(fmt::format("_agg_input_{}", queryState->aggExprCounter++));
            queryState->preAggProjections.emplace_back(tempName, argFn);
            return UnboundFieldAccessLogicalFunction{tempName};
        }();

        auto aggFuncExp = boundAgg->instantiateOnField(onField);
        if (!aggFuncExp.has_value())
        {
            throw std::move(aggFuncExp).error();
        }

        if (clauseMode == ScalarClauseMode::Having)
        {
            if (!queryState->forbiddenHavingAggregation.has_value())
            {
                queryState->forbiddenHavingAggregation = *aggFuncExp;
            }
            const auto dummyField = bindIdentifier(
                fmt::format("{}_{}", onField->getFieldName(), toUpperCase(boundAgg->aggregationName.getOriginalString())));
            return makeLogicalFunctionValue(LogicalFunction{UnboundFieldAccessLogicalFunction{dummyField}});
        }

        const auto funcUpper = toUpperCase(boundAgg->aggregationName.getOriginalString());
        const auto autoField = bindIdentifier(fmt::format("{}_{}", onField->getFieldName(), funcUpper));
        queryState->windowAggs.emplace_back(*aggFuncExp, autoField);
        if (clauseMode == ScalarClauseMode::SelectTopLevel)
        {
            queryState->lastTopLevelExprWasDirectAggregation = true;
        }
        return makeLogicalFunctionValue(LogicalFunction{UnboundFieldAccessLogicalFunction{autoField}});
    }

    NESValue bindFunctionCall(
        AntlrSQLParser::FunctionCallContext* callAST,
        const NestedScope& scope,
        QueryBindState* queryState,
        const ScalarClauseMode clauseMode) const
    {
        if (auto* const refAST = dynamic_cast<AntlrSQLParser::FunctionReferenceContext*>(callAST))
        {
            const auto id = bindIdentifier(refAST->identifier());
            const auto qid = QualifiedIdentifier::create(id);
            if (queryState == nullptr)
            {
                if (const auto found = scope.find(qid); found.has_value() && std::holds_alternative<NESValue>(*found))
                {
                    return std::get<NESValue>(*found);
                }
                return makeLogicalOperatorValue(LogicalOperator{SourceNameLogicalOperator::create(id)});
            }

            if (const auto found = scope.find(qid); found.has_value() && std::holds_alternative<NESValue>(*found))
            {
                return std::get<NESValue>(*found);
            }
            return makeLogicalFunctionValue(LogicalFunction{UnboundFieldAccessLogicalFunction{id}});
        }

        if (auto* const dtCallAST = dynamic_cast<AntlrSQLParser::DataTypeFunctionCallContext*>(callAST))
        {
            const auto normalized = normalizeBuiltinDataTypeName(dtCallAST->dataType->getText());
            const auto functorOpt = NESTypeCatalog::instance().findSqlFunctor(QualifiedIdentifier::create(Identifier::parse(normalized)));
            if (!functorOpt.has_value())
            {
                throw UnknownDataType("{}", dtCallAST->dataType->getText());
            }
            auto dtNesTypeExp = functorOpt->instantiate(NESTypeParameters{});
            if (!dtNesTypeExp.has_value())
            {
                throw std::move(dtNesTypeExp).error();
            }
            const auto args = dtCallAST->functionCallArgument();
            if (args.size() != 1)
            {
                throw InvalidQuerySyntax(
                    "Type constructor {} expects exactly 1 argument, got {} at {}",
                    normalized,
                    args.size(),
                    dtCallAST->getText());
            }
            auto* const scalarArg = dynamic_cast<AntlrSQLParser::ScalarFunctionArgContext*>(args.front());
            if (scalarArg == nullptr)
            {
                throw InvalidQuerySyntax("Expected scalar literal inside type constructor {} at {}", normalized, dtCallAST->getText());
            }
            auto constStr = extractTypedConstantArgument(scalarArg->openScalarExpression());
            const auto* trait = dtNesTypeExp->tryGetTrait<ScalarTypeTrait>();
            PRECONDITION(trait != nullptr, "Built-in DataType NESType must carry ScalarTypeTrait");
            const auto dataType = trait->tryGetDataType(dtNesTypeExp->getDescriptor()).value();
            return makeLogicalFunctionValue(LogicalFunction{ConstantValueLogicalFunction{dataType, std::move(constStr)}});
        }

        const auto childMode
            = (clauseMode == ScalarClauseMode::SelectTopLevel) ? ScalarClauseMode::SelectNested : clauseMode;

        if (auto* const emptyCallAST = dynamic_cast<AntlrSQLParser::EmptyFunctionCallContext*>(callAST))
        {
            const auto calleeType = resolveCalleeToInstantiatedType(emptyCallAST->callee, scope, {}, nullptr);
            auto constructedExp = calleeType.construct(NESValueParameters::positional({}));
            if (!constructedExp.has_value())
            {
                throw std::move(constructedExp).error();
            }
            return finalizeConstructedCallValue(std::move(*constructedExp), queryState, clauseMode, emptyCallAST->getText());
        }

        if (auto* const starCallAST = dynamic_cast<AntlrSQLParser::StarFunctionCallContext*>(callAST))
        {
            std::vector<NESValue> starArgs{makeAsteriskValue()};
            const auto calleeType = resolveCalleeToInstantiatedType(starCallAST->callee, scope, starArgs, nullptr);
            auto constructedExp = calleeType.construct(NESValueParameters::positional(std::move(starArgs)));
            if (!constructedExp.has_value())
            {
                throw std::move(constructedExp).error();
            }
            return finalizeConstructedCallValue(std::move(*constructedExp), queryState, clauseMode, starCallAST->getText());
        }

        if (auto* const posCallAST = dynamic_cast<AntlrSQLParser::PositionalFunctionCallContext*>(callAST))
        {
            std::vector<NESValue> argValues;
            argValues.reserve(posCallAST->functionCallArgument().size());
            for (auto* const argAST : posCallAST->functionCallArgument())
            {
                argValues.push_back(bindFunctionArgumentValue(argAST, scope, queryState, childMode));
            }
            const auto calleeType = resolveCalleeToInstantiatedType(posCallAST->callee, scope, argValues, nullptr);
            auto constructedExp = calleeType.construct(NESValueParameters::positional(std::move(argValues)));
            if (!constructedExp.has_value())
            {
                throw std::move(constructedExp).error();
            }
            return finalizeConstructedCallValue(std::move(*constructedExp), queryState, clauseMode, posCallAST->getText());
        }

        if (auto* const namedCallAST = dynamic_cast<AntlrSQLParser::NamedFunctionCallContext*>(callAST))
        {
            std::vector<NamedNESValue> namedArgs;
            namedArgs.reserve(namedCallAST->namedFunctionCallArgument().size());
            for (auto* const namedArgAST : namedCallAST->namedFunctionCallArgument())
            {
                auto argName = bindQualifiedIdentifier(namedArgAST->name);
                auto argVal = bindFunctionArgumentValue(namedArgAST->functionCallArgument(), scope, queryState, childMode);
                namedArgs.emplace_back(std::move(argName), std::move(argVal));
            }
            auto namedSchemaExp = Schema<NamedNESValue, Ordered>::tryCreateCollisionFree(std::move(namedArgs));
            if (!namedSchemaExp.has_value())
            {
                throw InvalidConfigParameter(
                    "Duplicate named argument in {}: {}",
                    namedCallAST->callee->getText(),
                    fmt::join(namedSchemaExp.error() | std::views::keys, ", "));
            }
            const auto calleeType = resolveCalleeToInstantiatedType(namedCallAST->callee, scope, {}, &*namedSchemaExp);
            auto constructedExp = calleeType.construct(NESValueParameters::named(std::move(*namedSchemaExp)));
            if (!constructedExp.has_value())
            {
                throw std::move(constructedExp).error();
            }
            return finalizeConstructedCallValue(std::move(*constructedExp), queryState, clauseMode, namedCallAST->getText());
        }

        INVARIANT(false, "Unhandled FunctionCallContext");
        std::unreachable();
    }

    NESValue bindScalarExpressionToValue(
        AntlrSQLParser::OpenScalarExpressionContext* exprAST,
        const NestedScope& scope,
        QueryBindState* queryState,
        const ScalarClauseMode clauseMode) const
    {
        if (auto* const parenAST = dynamic_cast<AntlrSQLParser::ParenthesizedScalarExpressionContext*>(exprAST))
        {
            const auto childMode = (clauseMode == ScalarClauseMode::SelectTopLevel) ? ScalarClauseMode::SelectNested : clauseMode;
            return bindScalarExpressionToValue(parenAST->openScalarExpression(), scope, queryState, childMode);
        }
        if (auto* const intervalAST = dynamic_cast<AntlrSQLParser::IntervalScalarExpressionContext*>(exprAST))
        {
            return makeTimeIntervalValue(bindIntervalExpression(intervalAST->intervalExpression(), scope, queryState, clauseMode));
        }
        if (auto* const litAST = dynamic_cast<AntlrSQLParser::LiteralScalarExpressionContext*>(exprAST))
        {
            if (auto* const schemaCtorAST = litAST->literal()->schemaCtor())
            {
                return makeSchemaNESValue(bindSchema(schemaCtorAST->schemaDefinition()));
            }
            auto* const scalarLit = litAST->literal()->scalarLiteral();
            if (queryState == nullptr)
            {
                return makeScalarLiteralNESValue(bindScalarLiteral(scalarLit));
            }
            if (auto* const strAST = scalarLit->stringLiteral())
            {
                if (strAST->getText().size() <= 2)
                {
                    throw InvalidQuerySyntax(
                        "A constant string literal must contain at least two quotes and must not be empty at {}",
                        exprAST->getText());
                }
            }
            return makeLogicalFunctionValue(createRawLiteralFunction(scalarLit->getText()));
        }
        if (auto* const unaryAST = dynamic_cast<AntlrSQLParser::UnaryArithmeticExpressionContext*>(exprAST))
        {
            if (queryState == nullptr && isDirectNumericLiteralAst(unaryAST))
            {
                const auto constStr = extractTypedConstantArgument(unaryAST);
                if (const auto intVal = from_chars<int64_t>(constStr))
                {
                    return makeScalarLiteralNESValue(Literal{*intVal});
                }
                if (const auto dblVal = from_chars<double>(constStr))
                {
                    return makeScalarLiteralNESValue(Literal{*dblVal});
                }
            }
        }
        if (auto* const callAST = dynamic_cast<AntlrSQLParser::FunctionCallScalarExpressionContext*>(exprAST))
        {
            return bindFunctionCall(callAST->functionCall(), scope, queryState, clauseMode);
        }
        return makeLogicalFunctionValue(bindOpenScalarExpression(exprAST, scope, queryState, clauseMode));
    }

    LogicalFunction bindOpenScalarExpression(
        AntlrSQLParser::OpenScalarExpressionContext* exprAST,
        const NestedScope& scope,
        QueryBindState* queryState,
        const ScalarClauseMode clauseMode) const
    {
        const auto nestedMode = (clauseMode == ScalarClauseMode::SelectTopLevel) ? ScalarClauseMode::SelectNested : clauseMode;

        if (auto* const parenAST = dynamic_cast<AntlrSQLParser::ParenthesizedScalarExpressionContext*>(exprAST))
        {
            return bindOpenScalarExpression(parenAST->openScalarExpression(), scope, queryState, nestedMode);
        }

        if (auto* const litAST = dynamic_cast<AntlrSQLParser::LiteralScalarExpressionContext*>(exprAST))
        {
            if (litAST->literal()->schemaCtor() != nullptr)
            {
                throw InvalidQuerySyntax("SCHEMA(...) is not a scalar expression: {}", exprAST->getText());
            }
            auto* const scalarLit = litAST->literal()->scalarLiteral();
            if (auto* const strAST = scalarLit->stringLiteral())
            {
                if (strAST->getText().size() <= 2)
                {
                    throw InvalidQuerySyntax(
                        "A constant string literal must contain at least two quotes and must not be empty at {}",
                        exprAST->getText());
                }
            }
            return createRawLiteralFunction(scalarLit->getText());
        }

        if (auto* const castAST = dynamic_cast<AntlrSQLParser::CastScalarExpressionContext*>(exprAST))
        {
            const auto targetType = bindDataType(castAST->targetType, DataType::NULLABLE::NOT_NULLABLE);
            auto innerFn = bindOpenScalarExpression(castAST->openScalarExpression(), scope, queryState, nestedMode);
            return CastToTypeLogicalFunction{targetType, std::move(innerFn)};
        }

        if (auto* const qualFieldAST = dynamic_cast<AntlrSQLParser::QualifiedFieldAccessExpressionContext*>(exprAST))
        {
            const auto qualifier = bindIdentifier(qualFieldAST->qualifier);
            const auto fieldName = bindIdentifier(qualFieldAST->fieldName);
            if (queryState != nullptr && !queryState->isJoinQuery && clauseMode != ScalarClauseMode::JoinOn)
            {
                queryState->sourceQualifiers.push_back(qualifier);
            }
            return UnboundFieldAccessLogicalFunction{fieldName};
        }

        if (auto* const callExprAST = dynamic_cast<AntlrSQLParser::FunctionCallScalarExpressionContext*>(exprAST))
        {
            auto val = bindFunctionCall(callExprAST->functionCall(), scope, queryState, clauseMode);
            auto fnExp = nesValueToLogicalFunction(val);
            if (!fnExp.has_value())
            {
                throw std::move(fnExp).error();
            }
            return std::move(*fnExp);
        }

        if (auto* const unaryAST = dynamic_cast<AntlrSQLParser::UnaryArithmeticExpressionContext*>(exprAST))
        {
            auto innerFn = bindOpenScalarExpression(unaryAST->openScalarExpression(), scope, queryState, nestedMode);
            if (unaryAST->op->getType() == AntlrSQLLexer::PLUS)
            {
                return innerFn;
            }
            if (isDirectNumericLiteralAst(unaryAST->openScalarExpression()))
            {
                if (const auto constFn = innerFn.tryGetAs<ConstantValueLogicalFunction>())
                {
                    return createNegatedNumericLiteralFunction(constFn->get());
                }
            }
            return MulLogicalFunction{
                ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::INT64), "-1"}, std::move(innerFn)};
        }

        if (auto* const mulAST = dynamic_cast<AntlrSQLParser::BinaryMultiplicativeExpressionContext*>(exprAST))
        {
            auto lhs = bindOpenScalarExpression(mulAST->left, scope, queryState, nestedMode);
            auto rhs = bindOpenScalarExpression(mulAST->right, scope, queryState, nestedMode);
            switch (mulAST->op->getType())
            {
                case AntlrSQLLexer::ASTERISK:
                    return MulLogicalFunction{std::move(lhs), std::move(rhs)};
                case AntlrSQLLexer::DIV:
                    return DivLogicalFunction{std::move(lhs), std::move(rhs)};
                case AntlrSQLLexer::PERCENT:
                    return ModuloLogicalFunction{std::move(lhs), std::move(rhs)};
                default:
                    throw InvalidQuerySyntax("Unknown multiplicative operator: {}", mulAST->op->getText());
            }
        }

        if (auto* const addAST = dynamic_cast<AntlrSQLParser::BinaryAdditiveExpressionContext*>(exprAST))
        {
            auto lhs = bindOpenScalarExpression(addAST->left, scope, queryState, nestedMode);
            auto rhs = bindOpenScalarExpression(addAST->right, scope, queryState, nestedMode);
            switch (addAST->op->getType())
            {
                case AntlrSQLLexer::PLUS:
                    return AddLogicalFunction{std::move(lhs), std::move(rhs)};
                case AntlrSQLLexer::MINUS:
                    return SubLogicalFunction{std::move(lhs), std::move(rhs)};
                default:
                    throw InvalidQuerySyntax("Unknown additive operator: {}", addAST->op->getText());
            }
        }

        if (auto* const cmpAST = dynamic_cast<AntlrSQLParser::BinaryComparisonExpressionContext*>(exprAST))
        {
            auto lhs = bindOpenScalarExpression(cmpAST->left, scope, queryState, nestedMode);
            auto rhs = bindOpenScalarExpression(cmpAST->right, scope, queryState, nestedMode);
            switch (cmpAST->op->getType())
            {
                case AntlrSQLLexer::EQ:
                    return EqualsLogicalFunction{std::move(lhs), std::move(rhs)};
                case AntlrSQLLexer::NEQ:
                    return NegateLogicalFunction{EqualsLogicalFunction{std::move(lhs), std::move(rhs)}};
                case AntlrSQLLexer::LESSER:
                    return LessLogicalFunction{std::move(lhs), std::move(rhs)};
                case AntlrSQLLexer::LESSER_EQUALS:
                    return LessEqualsLogicalFunction{std::move(lhs), std::move(rhs)};
                case AntlrSQLLexer::GREATER:
                    return GreaterLogicalFunction{std::move(lhs), std::move(rhs)};
                case AntlrSQLLexer::GREATER_EQUALS:
                    return GreaterEqualsLogicalFunction{std::move(lhs), std::move(rhs)};
                default:
                    throw InvalidQuerySyntax("Unknown comparison operator: {}", cmpAST->op->getText());
            }
        }

        if (auto* const isNullAST = dynamic_cast<AntlrSQLParser::IsNullExpressionContext*>(exprAST))
        {
            LogicalFunction fn = IsNullCheckLogicalFunction{bindOpenScalarExpression(isNullAST->expr, scope, queryState, nestedMode)};
            if (isNullAST->negated != nullptr)
            {
                fn = NegateLogicalFunction{std::move(fn)};
            }
            return fn;
        }

        if (auto* const isNaNAST = dynamic_cast<AntlrSQLParser::IsNaNExpressionContext*>(exprAST))
        {
            LogicalFunction fn = IsNaNLogicalFunction{bindOpenScalarExpression(isNaNAST->expr, scope, queryState, nestedMode)};
            if (isNaNAST->negated != nullptr)
            {
                fn = NegateLogicalFunction{std::move(fn)};
            }
            return fn;
        }

        if (auto* const betweenAST = dynamic_cast<AntlrSQLParser::BetweenExpressionContext*>(exprAST))
        {
            const auto valFn = bindOpenScalarExpression(betweenAST->expr, scope, queryState, nestedMode);
            auto lowerFn = bindOpenScalarExpression(betweenAST->lower, scope, queryState, nestedMode);
            auto upperFn = bindOpenScalarExpression(betweenAST->upper, scope, queryState, nestedMode);
            LogicalFunction fn = AndLogicalFunction{
                GreaterEqualsLogicalFunction{valFn, std::move(lowerFn)},
                LessEqualsLogicalFunction{valFn, std::move(upperFn)}};
            if (betweenAST->negated != nullptr)
            {
                fn = NegateLogicalFunction{std::move(fn)};
            }
            return fn;
        }

        if (auto* const inListAST = dynamic_cast<AntlrSQLParser::InListExpressionContext*>(exprAST))
        {
            const auto valFn = bindOpenScalarExpression(inListAST->expr, scope, queryState, nestedMode);
            if (inListAST->candidates.empty())
            {
                throw InvalidQuerySyntax("IN predicate requires at least one candidate value: {}", inListAST->getText());
            }
            LogicalFunction fn = EqualsLogicalFunction{
                valFn, bindOpenScalarExpression(inListAST->candidates.front(), scope, queryState, nestedMode)};
            for (auto* const candAST : inListAST->candidates | std::views::drop(1))
            {
                fn = OrLogicalFunction{
                    std::move(fn), EqualsLogicalFunction{valFn, bindOpenScalarExpression(candAST, scope, queryState, nestedMode)}};
            }
            if (inListAST->negated != nullptr)
            {
                fn = NegateLogicalFunction{std::move(fn)};
            }
            return fn;
        }

        if (auto* const inSubqAST = dynamic_cast<AntlrSQLParser::UnsupportedInSubqueryExpressionContext*>(exprAST))
        {
            throw UnsupportedQuery("IN subqueries are currently not supported: {}", inSubqAST->getText());
        }

        if (auto* const isTruthAST = dynamic_cast<AntlrSQLParser::UnsupportedIsTruthExpressionContext*>(exprAST))
        {
            const auto keyword = toUpperCase(isTruthAST->kind->getText());
            const auto* const neg = isTruthAST->negated != nullptr ? "NOT " : "";
            throw UnsupportedQuery("IS {}{} is not supported: {}", neg, keyword, isTruthAST->getText());
        }

        if (auto* const isDistAST = dynamic_cast<AntlrSQLParser::UnsupportedIsDistinctExpressionContext*>(exprAST))
        {
            const auto* const neg = isDistAST->negated != nullptr ? "NOT " : "";
            throw UnsupportedQuery("IS {}DISTINCT FROM is not supported: {}", neg, isDistAST->getText());
        }

        if (auto* const likeAST = dynamic_cast<AntlrSQLParser::UnsupportedLikeExpressionContext*>(exprAST))
        {
            const auto keyword = toUpperCase(likeAST->kind->getText());
            const auto* const neg = likeAST->negated != nullptr ? "NOT " : "";
            throw UnsupportedQuery("{}{} is not supported: {}", neg, keyword, likeAST->getText());
        }

        if (auto* const notAST = dynamic_cast<AntlrSQLParser::LogicalNotExpressionContext*>(exprAST))
        {
            return NegateLogicalFunction{bindOpenScalarExpression(notAST->openScalarExpression(), scope, queryState, nestedMode)};
        }

        if (auto* const andAST = dynamic_cast<AntlrSQLParser::LogicalAndExpressionContext*>(exprAST))
        {
            return AndLogicalFunction{
                bindOpenScalarExpression(andAST->left, scope, queryState, nestedMode),
                bindOpenScalarExpression(andAST->right, scope, queryState, nestedMode)};
        }

        if (auto* const orAST = dynamic_cast<AntlrSQLParser::LogicalOrExpressionContext*>(exprAST))
        {
            return OrLogicalFunction{
                bindOpenScalarExpression(orAST->left, scope, queryState, nestedMode),
                bindOpenScalarExpression(orAST->right, scope, queryState, nestedMode)};
        }

        throw InvalidQuerySyntax("Unsupported scalar expression: {}", exprAST->getText());
    }

    std::optional<Identifier> extractDirectCalleeIdentifier(AntlrSQLParser::FunctionCallContext* callAST) const
    {
        AntlrSQLParser::FunctionCallContext* callee = nullptr;
        if (auto* const emptyCall = dynamic_cast<AntlrSQLParser::EmptyFunctionCallContext*>(callAST))
        {
            callee = emptyCall->callee;
        }
        else if (auto* const namedCall = dynamic_cast<AntlrSQLParser::NamedFunctionCallContext*>(callAST))
        {
            callee = namedCall->callee;
        }
        else if (auto* const posCall = dynamic_cast<AntlrSQLParser::PositionalFunctionCallContext*>(callAST))
        {
            callee = posCall->callee;
        }
        if (auto* const calleeRef = dynamic_cast<AntlrSQLParser::FunctionReferenceContext*>(callee))
        {
            return bindIdentifier(calleeRef->identifier());
        }
        return std::nullopt;
    }

    std::pair<LogicalPlan, std::optional<Identifier>>
    bindClosedRelationalExpression(AntlrSQLParser::ClosedRelationalExpressionContext* closedRelAST, const NestedScope& scope) const
    {
        if (auto* const unionSeqAST = closedRelAST->unionSeq())
        {
            return {bindUnionSeq(unionSeqAST, scope), std::nullopt};
        }
        if (auto* const openRelAST = closedRelAST->openRelationalExpression())
        {
            return bindOpenRelationalExpression(openRelAST, scope);
        }
        if (auto* const callAST = closedRelAST->functionCall())
        {
            if (auto* const bareRef = dynamic_cast<AntlrSQLParser::FunctionReferenceContext*>(callAST))
            {
                const auto srcId = bindIdentifier(bareRef->identifier());
                return {LogicalPlanBuilder::createLogicalPlan(srcId), srcId};
            }

            if (const auto calleeId = extractDirectCalleeIdentifier(callAST);
                calleeId.has_value() && !scope.find(QualifiedIdentifier::create(*calleeId)).has_value())
            {
                throw UnknownSourceType(
                    "The source type '{}' is not registered. If it is a plugin, make sure you activated it.", *calleeId);
            }

            const auto val = bindFunctionCall(callAST, scope, /*queryState=*/nullptr, ScalarClauseMode::Where);
            if (const auto* boundAnonSource = val.tryGetAs<BoundAnonymousSource>())
            {
                auto resolvedExp = boundAnonSource->resolve(defaultConfigValues, configTransformations);
                if (!resolvedExp.has_value())
                {
                    throw std::move(resolvedExp).error();
                }
                auto& [generalConfig, pluginConfig, inputFormatter, schema] = *resolvedExp;
                return {
                    LogicalPlanBuilder::createLogicalPlan(
                        std::move(generalConfig), std::move(pluginConfig), std::move(inputFormatter), std::move(schema)),
                    std::nullopt};
            }
            if (const auto* op = val.tryGetAs<LogicalOperator>())
            {
                return {LogicalPlan{QueryId::invalid(), {*op}}, std::nullopt};
            }
            throw InvalidQuerySyntax(
                "FROM expression '{}' does not evaluate to a relational stream or source", callAST->getText());
        }
        INVARIANT(false, "Unhandled ClosedRelationalExpressionContext");
        std::unreachable();
    }

    std::pair<LogicalPlan, std::optional<Identifier>>
    bindClosedNamedRelationalExpression(AntlrSQLParser::ClosedNamedRelationalExpressionContext* namedRelAST, const NestedScope& scope) const
    {
        auto [plan, sourceIdOpt] = bindClosedRelationalExpression(namedRelAST->closedRelationalExpression(), scope);
        if (namedRelAST->alias != nullptr)
        {
            return {std::move(plan), bindIdentifier(namedRelAST->alias)};
        }
        return {std::move(plan), std::move(sourceIdOpt)};
    }

    JoinLogicalOperator::JoinType bindJoinType(AntlrSQLParser::InbuildJoinsContext* joinTypeAST) const
    {
        if (auto* const outerAST = joinTypeAST->outerJoinsTypes())
        {
            if (outerAST->LEFT() != nullptr)
            {
                return JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN;
            }
            if (outerAST->RIGHT() != nullptr)
            {
                return JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN;
            }
            if (outerAST->FULL() != nullptr)
            {
                return JoinLogicalOperator::JoinType::OUTER_FULL_JOIN;
            }
        }
        return JoinLogicalOperator::JoinType::INNER_JOIN;
    }

    BinaryWindowDefinition
    bindBinaryWindowTerm(AntlrSQLParser::BinaryWindowTermContext* binaryWindowAST, const NestedScope& scope) const
    {
        QueryBindState dummyWindowState{};
        dummyWindowState.isJoinQuery = true;
        const auto binaryWinScope = withBinaryWindowScope(scope);
        const auto val = bindFunctionCall(binaryWindowAST->functionCall(), binaryWinScope, &dummyWindowState, ScalarClauseMode::Window);
        const auto* defPtr = val.tryGetAs<BinaryWindowDefinition>();
        if (defPtr == nullptr)
        {
            throw InvalidQuerySyntax("Invalid binary join window specification: {}", binaryWindowAST->getText());
        }
        return *defPtr;
    }

    LogicalPlan
    bindJoinRelationalExpression(AntlrSQLParser::JoinRelationalExpressionContext* joinAST, const NestedScope& scope) const
    {
        auto leftPlan = [&]() -> LogicalPlan
        {
            if (joinAST->left != nullptr)
            {
                return bindClosedNamedRelationalExpression(joinAST->left, scope).first;
            }
            return bindJoinRelationalExpression(joinAST->leftJoin, scope);
        }();
        auto rightPlan = bindClosedNamedRelationalExpression(joinAST->right, scope).first;

        QueryBindState joinCondState{};
        joinCondState.isJoinQuery = true;
        auto joinCondition = bindOpenScalarExpression(joinAST->condition, scope, &joinCondState, ScalarClauseMode::JoinOn);
        const auto joinType = bindJoinType(joinAST->inbuildJoins());
        auto binaryWin = bindBinaryWindowTerm(joinAST->binaryWindowTerm(), scope);

        return LogicalPlanBuilder::addJoin(
            std::move(leftPlan),
            std::move(rightPlan),
            std::move(joinCondition),
            binaryWin.windowType,
            joinType,
            binaryWin.timeCharacteristics[0],
            binaryWin.timeCharacteristics[1]);
    }

    std::pair<LogicalPlan, std::optional<Identifier>>
    bindOpenRelationalExpression(AntlrSQLParser::OpenRelationalExpressionContext* openRelAST, const NestedScope& scope) const
    {
        if (auto* const closedAST = openRelAST->closedRelationalExpression())
        {
            return bindClosedRelationalExpression(closedAST, scope);
        }
        if (auto* const joinAST = openRelAST->joinRelationalExpression())
        {
            return {bindJoinRelationalExpression(joinAST, scope), std::nullopt};
        }
        INVARIANT(false, "Unhandled OpenRelationalExpressionContext");
        std::unreachable();
    }

    UnaryWindowDefinition
    bindUnaryWindowTerm(AntlrSQLParser::WindowTermContext* windowAST, const NestedScope& scope, QueryBindState& queryState) const
    {
        const auto unaryWinScope = withUnaryWindowScope(scope);
        const auto val = bindFunctionCall(windowAST->functionCall(), unaryWinScope, &queryState, ScalarClauseMode::Window);
        const auto* defPtr = val.tryGetAs<UnaryWindowDefinition>();
        if (defPtr == nullptr)
        {
            throw InvalidQuerySyntax("Invalid window specification: {}", windowAST->getText());
        }
        return *defPtr;
    }

    static void validateSourceQualifiers(const QueryBindState& state)
    {
        if (state.isJoinQuery || state.sourceQualifiers.empty())
        {
            return;
        }
        if (!state.expectedQualifier.has_value())
        {
            throw InvalidQuerySyntax("Qualified field references require a named source");
        }
        for (const auto& qualifier : state.sourceQualifiers)
        {
            if (qualifier != *state.expectedQualifier)
            {
                throw InvalidQuerySyntax("Unknown source qualifier {}, expected {}", qualifier, *state.expectedQualifier);
            }
        }
    }

    static void validateAggregatedSelectList(
        const QueryBindState& state,
        const std::vector<std::pair<std::optional<Identifier>, LogicalFunction>>& rawProjections)
    {
        const auto isAvailableAfterAggregation = [&state](const Identifier& field)
        {
            return field == Identifier::parse("start") || field == Identifier::parse("end")
                || std::ranges::any_of(
                    state.groupByFields, [&field](const auto& key) { return key.getFieldName() == field; })
                || std::ranges::any_of(
                    state.windowAggs, [&field](const auto& agg) { return agg.second == field; });
        };

        for (const auto& projectionFn : rawProjections | std::views::values)
        {
            for (const auto& fn : BFSRange{projectionFn})
            {
                if (const auto access = fn.tryGetAs<UnboundFieldAccessLogicalFunction>();
                    access.has_value() && !isAvailableAfterAggregation(access.value()->getFieldName()))
                {
                    throw InvalidQuerySyntax(
                        R"(column "{}" must appear in the GROUP BY clause or be used in an aggregate function)",
                        access.value()->getFieldName().getOriginalString());
                }
            }
        }
    }

    LogicalPlan bindQuery(AntlrSQLParser::QueryContext* queryAst, const NestedScope& scope) const
    {
        QueryBindState state{};

        /// 1. FROM
        auto* const fromRelAST = queryAst->fromTerm()->fromRelation();
        LogicalPlan queryPlan = [&]() -> LogicalPlan
        {
            if (auto* const namedRel = fromRelAST->closedNamedRelationalExpression())
            {
                auto [plan, qualifierOpt] = bindClosedNamedRelationalExpression(namedRel, scope);
                state.expectedQualifier = std::move(qualifierOpt);
                return std::move(plan);
            }
            state.isJoinQuery = true;
            return bindJoinRelationalExpression(fromRelAST->joinRelationalExpression(), scope);
        }();

        /// 2. WHERE
        if (auto* const filterAST = queryAst->filterTerm())
        {
            auto whereFn = bindOpenScalarExpression(filterAST->openScalarExpression(), scope, &state, ScalarClauseMode::Where);
            queryPlan = LogicalPlanBuilder::addSelection(std::move(whereFn), queryPlan);
        }

        /// 3. WINDOW & GROUP BY
        std::optional<UnaryWindowDefinition> windowDefOpt;
        if (auto* const winAST = queryAst->windowTerm())
        {
            windowDefOpt = bindUnaryWindowTerm(winAST, scope, state);
        }
        if (auto* const groupByAST = queryAst->groupByTerm())
        {
            if (!windowDefOpt.has_value())
            {
                throw InvalidQuerySyntax("GROUP BY requires a WINDOW clause");
            }
            for (auto* const groupExprAST : groupByAST->openScalarExpression())
            {
                auto groupFn = bindOpenScalarExpression(groupExprAST, scope, &state, ScalarClauseMode::GroupBy);
                const auto fieldOpt = groupFn.tryGetAs<UnboundFieldAccessLogicalFunction>();
                if (!fieldOpt.has_value())
                {
                    throw InvalidQuerySyntax("GROUP BY expression must be a column identifier, got {}", groupExprAST->getText());
                }
                state.groupByFields.push_back(fieldOpt->get());
            }
        }

        /// 4. SELECT projections
        bool asterisk = false;
        std::vector<std::pair<std::optional<Identifier>, LogicalFunction>> rawProjections;
        for (auto* const projAST : queryAst->projectionTerm()->projectionSeq()->projection())
        {
            if (dynamic_cast<AntlrSQLParser::AsteriskProjectionContext*>(projAST) != nullptr)
            {
                asterisk = true;
                continue;
            }
            if (auto* const qualStarAST = dynamic_cast<AntlrSQLParser::QualifiedAsteriskProjectionContext*>(projAST))
            {
                state.sourceQualifiers.push_back(bindIdentifier(qualStarAST->qualifier));
                asterisk = true;
                continue;
            }
            auto* const exprProjAST = dynamic_cast<AntlrSQLParser::ExpressionProjectionContext*>(projAST);
            INVARIANT(exprProjAST != nullptr, "Unhandled ProjectionContext");

            state.lastTopLevelExprWasDirectAggregation = false;
            auto exprFn = bindOpenScalarExpression(
                exprProjAST->openScalarExpression(), scope, &state, ScalarClauseMode::SelectTopLevel);

            if (state.lastTopLevelExprWasDirectAggregation)
            {
                if (exprProjAST->alias != nullptr)
                {
                    const auto aliasId = bindIdentifier(exprProjAST->alias);
                    state.windowAggs.back().second = aliasId;
                    state.userAggregationAliases.push_back(aliasId);
                    rawProjections.emplace_back(aliasId, UnboundFieldAccessLogicalFunction{aliasId});
                }
                else
                {
                    rawProjections.emplace_back(std::nullopt, std::move(exprFn));
                }
            }
            else
            {
                if (exprProjAST->alias != nullptr)
                {
                    rawProjections.emplace_back(bindIdentifier(exprProjAST->alias), std::move(exprFn));
                }
                else if (const auto fieldAccess = exprFn.tryGetAs<UnboundFieldAccessLogicalFunction>())
                {
                    rawProjections.emplace_back(fieldAccess.value()->getFieldName(), std::move(exprFn));
                }
                else
                {
                    rawProjections.emplace_back(std::nullopt, std::move(exprFn));
                }
            }
        }

        if (!state.windowAggs.empty() && !windowDefOpt.has_value())
        {
            throw InvalidQuerySyntax("Aggregation functions require a WINDOW clause");
        }

        /// 5. HAVING (bound before source-qualifier validation so qualifier checks cover HAVING too)
        std::optional<LogicalFunction> havingFnOpt;
        if (auto* const havingAST = queryAst->havingTerm())
        {
            if (!windowDefOpt.has_value())
            {
                throw InvalidQuerySyntax("HAVING requires a WINDOW clause");
            }
            auto havingFn = bindOpenScalarExpression(havingAST->openScalarExpression(), scope, &state, ScalarClauseMode::Having);
            if (state.forbiddenHavingAggregation.has_value())
            {
                const auto& havingAgg = *state.forbiddenHavingAggregation;
                const auto expression = describeAggregation(havingAgg, state.preAggProjections);
                const auto projectedAlias = [&]() -> std::optional<Identifier>
                {
                    for (const auto& [agg, alias] : state.windowAggs)
                    {
                        if (agg == havingAgg && std::ranges::contains(state.userAggregationAliases, alias))
                        {
                            return alias;
                        }
                    }
                    return std::nullopt;
                }();
                if (projectedAlias.has_value())
                {
                    throw InvalidQuerySyntax(
                        "HAVING must use the projected alias: write {} instead of {}",
                        projectedAlias->getOriginalString(),
                        expression);
                }
                throw InvalidQuerySyntax(
                    "HAVING can only use fields the SELECT list projects: add {} AS <alias> to the SELECT list and use <alias> in HAVING",
                    expression);
            }
            havingFnOpt = std::move(havingFn);
        }

        validateSourceQualifiers(state);

        /// 6. Pre-aggregation projection (for desugared expressions like SUM(i + UINT64(1)))
        if (!state.preAggProjections.empty())
        {
            queryPlan = LogicalPlanBuilder::addProjection(state.preAggProjections, /*asterisk=*/true, queryPlan);
        }

        /// 7. Windowed aggregation
        if (windowDefOpt.has_value())
        {
            validateAggregatedSelectList(state, rawProjections);
            auto projectedAggs = state.windowAggs
                | std::views::transform(
                                     [](const auto& aggPair)
                                     { return WindowedAggregationLogicalOperator::ProjectedAggregation{aggPair.first, aggPair.second}; })
                | std::ranges::to<std::vector>();
            queryPlan = LogicalPlanBuilder::addWindowAggregation(
                queryPlan,
                windowDefOpt->windowType,
                std::move(projectedAggs),
                state.groupByFields,
                windowDefOpt->timeCharacteristic);
        }

        /// 8. Final SELECT projection
        auto finalProjections = rawProjections
            | std::views::transform(
                                    [this](const auto& pair)
                                    {
                                        if (!pair.first.has_value())
                                        {
                                            return ProjectionLogicalOperator::UnboundProjection{
                                                bindIdentifier(pair.second.explain(ExplainVerbosity::Short)), pair.second};
                                        }
                                        return ProjectionLogicalOperator::UnboundProjection{*pair.first, pair.second};
                                    })
            | std::ranges::to<std::vector>();
        queryPlan = LogicalPlanBuilder::addProjection(std::move(finalProjections), asterisk, queryPlan);

        /// 9. Post-projection HAVING selection
        if (havingFnOpt.has_value())
        {
            queryPlan = LogicalPlanBuilder::addSelection(std::move(*havingFnOpt), queryPlan);
        }

        return queryPlan;
    }

    LogicalPlan bindUnionSeq(AntlrSQLParser::UnionSeqContext* unionSeq, const NestedScope& scope) const
    {
        const auto queries = unionSeq->query();
        PRECONDITION(!queries.empty(), "unionSeq must contain at least one query");
        auto plan = bindQuery(queries.front(), scope);
        for (auto* const nextQueryAST : queries | std::views::drop(1))
        {
            auto rightPlan = bindQuery(nextQueryAST, scope);
            plan = LogicalPlanBuilder::addUnion(std::move(plan), std::move(rightPlan));
        }
        return plan;
    }

    LogicalPlan bindSink(AntlrSQLParser::IntoTermContext* intoAST, LogicalPlan queryPlan, const NestedScope& scope) const
    {
        if (intoAST->sink().empty())
        {
            throw InvalidQuerySyntax("INTO must be followed by at least one sink");
        }
        auto* const firstSinkCall = intoAST->sink().front()->functionCall();
        if (auto* const bareRef = dynamic_cast<AntlrSQLParser::FunctionReferenceContext*>(firstSinkCall))
        {
            return LogicalPlanBuilder::addSink(bindIdentifier(bareRef->identifier()), queryPlan);
        }

        if (const auto calleeId = extractDirectCalleeIdentifier(firstSinkCall);
            calleeId.has_value() && !scope.find(QualifiedIdentifier::create(*calleeId)).has_value())
        {
            throw UnknownSinkType(
                "The sink type '{}' is not registered. If it is a plugin, make sure you activated it.", *calleeId);
        }

        const auto val = bindFunctionCall(firstSinkCall, scope, /*queryState=*/nullptr, ScalarClauseMode::Where);
        if (const auto* boundAnonSink = val.tryGetAs<BoundAnonymousSink>())
        {
            auto resolvedExp = boundAnonSink->resolve(defaultConfigValues, configTransformations);
            if (!resolvedExp.has_value())
            {
                throw std::move(resolvedExp).error();
            }
            auto& [generalSinkConfig, sinkSchema, pluginSinkConfig, outputFormatterDescriptor] = *resolvedExp;
            return LogicalPlanBuilder::addAnonymousSink(
                std::move(sinkSchema),
                std::move(generalSinkConfig),
                std::move(pluginSinkConfig),
                std::move(outputFormatterDescriptor),
                queryPlan);
        }
        throw InvalidQuerySyntax("INTO target '{}' does not evaluate to a sink", firstSinkCall->getText());
    }

    LogicalPlan bindQueryWithSink(AntlrSQLParser::QueryWithSinkContext* queryAst, const NestedScope& scope) const
    {
        INVARIANT(queryAst->unionSeq() != nullptr, "Should be enforced by antlr");
        if (queryAst->intoTerm() == nullptr)
        {
            throw InvalidQuerySyntax("A query must specify an INTO sink clause");
        }
        auto plan = bindUnionSeq(queryAst->unionSeq(), scope);
        return bindSink(queryAst->intoTerm(), std::move(plan), scope);
    }

    std::expected<Statement, Exception> bind(AntlrSQLParser::StatementContext* statementAST) const
    {
        try
        {
            if (auto* const createAST = statementAST->createStatement(); createAST != nullptr)
            {
                return bindCreateStatement(createAST);
            }
            if (auto* const showAST = statementAST->showStatement(); showAST != nullptr)
            {
                return bindShowStatement(showAST);
            }
            if (auto* const dropAst = statementAST->dropStatement(); dropAst != nullptr)
            {
                return bindDropStatement(dropAst);
            }
            if (auto* const explainStatementAST = statementAST->explainStatement())
            {
                return bindExplainStatement(explainStatementAST);
            }
            if (auto* const queryAst = statementAST->queryWithOptions(); queryAst != nullptr)
            {
                std::optional<DistributedQueryId> queryId;
                if (queryAst->optionsClause() != nullptr)
                {
                    auto options = bindConfigOptions(getConfigOptions(queryAst->optionsClause()));
                    static const auto QueryIdentifier = Identifier::parse("QUERY");
                    static const auto IdIdentifier = Identifier::parse("ID");
                    if (auto optionsIter = options.find(QueryIdentifier); optionsIter != options.end())
                    {
                        if (auto idIter = optionsIter->second.find(IdIdentifier); idIter != optionsIter->second.end())
                        {
                            const auto* literal = std::get_if<Literal>(&idIter->second);
                            if (literal == nullptr || !std::holds_alternative<std::string>(*literal))
                            {
                                throw InvalidQuerySyntax("Query id must be a string");
                            }
                            queryId = DistributedQueryId(std::get<std::string>(*literal));
                        }
                    }
                }
                const auto rootScope = buildRootScope();
                return QueryStatement{.plan = bindQueryWithSink(queryAst->queryWithSink(), rootScope), .id = std::move(queryId)};
            }

            throw InvalidStatement(statementAST->toString());
        }
        catch (Exception& e)
        {
            return std::unexpected{e};
        }
        catch (const std::exception& e)
        {
            return std::unexpected{InvalidStatement(e.what())};
        }
    }
};

StatementBinder::StatementBinder(
    std::shared_ptr<const SourceCatalog> sourceCatalog,
    std::shared_ptr<const SinkCatalog> sinkCatalog,
    std::shared_ptr<const ModelCatalog> modelCatalog,
    Schema<ConfigFieldDefault, Ordered> defaultConfigValues,
    Schema<ConfigFieldTransformation, Unordered> configTransformations)
    : impl(std::make_unique<Impl>(
          std::move(sourceCatalog),
          std::move(sinkCatalog),
          std::move(modelCatalog),
          std::move(defaultConfigValues),
          std::move(configTransformations)))
{
}

StatementBinder::StatementBinder(StatementBinder&& other) noexcept : impl(std::move(other.impl))
{
}

StatementBinder& StatementBinder::operator=(StatementBinder&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    impl = std::move(other.impl);
    return *this;
}

StatementBinder::~StatementBinder() = default;

std::expected<Statement, Exception> StatementBinder::bind(AntlrSQLParser::StatementContext* statementAST) const
{
    return impl->bind(statementAST);
}

std::expected<void, Exception> StatementBinder::parseAndBindEach(
    const std::string_view statementString,
    const std::function<void(std::expected<Statement, Exception>)>& onStatement) const
{
    try
    {
        ThrowingSyntaxErrorListener errorListener;
        antlr4::ANTLRInputStream input(statementString.data(), statementString.length());
        AntlrSQLLexer lexer(&input);
        antlr4::CommonTokenStream tokens(&lexer);
        AntlrSQLParser parser(&tokens);
        lexer.removeErrorListeners();
        parser.removeErrorListeners();
        lexer.addErrorListener(&errorListener);
        parser.addErrorListener(&errorListener);
        parser.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());
        AntlrSQLParser::MultipleStatementsContext* tree = parser.multipleStatements();
        if (tree == nullptr)
        {
            return std::unexpected{InvalidQuerySyntax("{}", statementString)};
        }

        for (auto* const statementAST : tree->statement())
        {
            onStatement(impl->bind(statementAST));
        }
        return {};
    }
    catch (const Exception& e)
    {
        return std::unexpected{e};
    }
    catch (antlr4::ParseCancellationException& e)
    {
        return std::unexpected{InvalidQuerySyntax("{}", e)};
    }
}

std::expected<std::vector<std::expected<Statement, Exception>>, Exception>
StatementBinder::parseAndBind(const std::string_view statementString) const
{
    std::vector<std::expected<Statement, Exception>> bound;
    const auto parseRes = parseAndBindEach(statementString, [&bound](std::expected<Statement, Exception> stmt) { bound.push_back(std::move(stmt)); });
    if (!parseRes.has_value())
    {
        return std::unexpected{parseRes.error()};
    }
    return bound;
}

std::expected<Statement, Exception> StatementBinder::parseAndBindSingle(const std::string_view statementStrings) const
{
    auto allParsed = parseAndBind(statementStrings);
    if (allParsed.has_value())
    {
        if (allParsed->size() > 1)
        {
            return std::unexpected{InvalidQuerySyntax("Expected a single statement, but got multiple")};
        }
        if (allParsed->empty())
        {
            return std::unexpected{InvalidQuerySyntax("Expected a single statement, but got none")};
        }
        return allParsed->at(0);
    }
    return std::unexpected{allParsed.error()};
}

std::expected<LogicalPlan, Exception> StatementBinder::parseAndBindQuery(const std::string_view queryString) const
{
    auto single = parseAndBindSingle(queryString);
    if (!single.has_value())
    {
        return std::unexpected{single.error()};
    }
    if (auto* queryStmt = std::get_if<QueryStatement>(&*single))
    {
        return std::move(queryStmt->plan);
    }
    return std::unexpected{InvalidQuerySyntax("Expected a SELECT query statement")};
}

LogicalPlan StatementBinder::createLogicalQueryPlanFromSQLString(const std::string_view queryString)
{
    const StatementBinder binder{};
    auto planExp = binder.parseAndBindQuery(queryString);
    if (!planExp.has_value())
    {
        throw std::move(planExp).error();
    }
    return std::move(*planExp);
}

std::ostream& operator<<(std::ostream& os, const CreatePhysicalSourceStatement& obj)
{
    return os << fmt::format(
               "CreatePhysicalSourceStatement({}, sourceType: {}, inputFormatterType: {}, logicalSource: {})",
               obj.generalSourceConfig,
               obj.pluginSourceConfig.getType(),
               obj.pluginInputFormatterConfig.getInputFormatterType(),
               obj.logicalSourceName);
}

/// NOLINTEND(readability-convert-member-functions-to-static)
}
