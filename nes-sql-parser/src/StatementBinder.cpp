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

#include <Configurations/ConfigField.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Identifiers/Identifiers.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Optional.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>
#include <Util/Variant.hpp>
#include <fmt/format.h>
#include "Functions/ConstantValueLogicalFunction.hpp"
#include "Operators/ProjectionLogicalOperator.hpp"
#include "Operators/SelectionLogicalOperator.hpp"
#include "Operators/UnionLogicalOperator.hpp"
#include "Operators/Windows/WindowedAggregationLogicalOperator.hpp"

#include <ANTLRInputStream.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <BailErrorStrategy.h>
#include <CommonTokenStream.h>
#include <Exceptions.h>
#include <DataTypes/DataType.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/URI.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <openvino/runtime/properties.hpp>

namespace NES
{

namespace
{
using Literal = std::variant<std::string, int64_t, uint64_t, double, bool, std::monostate>;
/// TODO #764 use identifier lists instead of map of maps
using ConfigMap
    = std::unordered_map<Identifier, std::unordered_map<Identifier, std::variant<Literal, Schema<UnqualifiedUnboundField, Ordered>>>>;
using ConfigMultiMap = std::vector<std::pair<QualifiedIdentifier, std::variant<Literal, Schema<UnqualifiedUnboundField, Ordered>>>>;

class TimeInterval;

class UnaryWindowDefinition
{
    Windowing::TimeBasedWindowType windowType;
    //Windowing::TimeCharacteristic timeCharacteristic;
};

class BinaryWindowDefinition
{
    Windowing::TimeBasedWindowType windowType;
    std::array<Windowing::TimeCharacteristic, 2> timeCharacteristcs;
    //Windowing::TimeCharacteristic leftCharacteristics;
    //Windowing::TimeCharacteristic rightCharacteristics;
};
enum class DatetimeType : uint8_t
{
    YEAR,
    MONTH,
    DAY,
    HOUR,
    MINUTE,
    SECOND,
    MILLISECOND
};

class TimeInterval
{
    DatetimeType timeType;
    uint64_t intervalSize;

    TimeInterval(const DatetimeType timeType, const uint64_t intervalSize) : timeType(timeType), intervalSize(intervalSize) { }

public:
    [[nodiscard]] const DatetimeType& getTimeType() const { return timeType; }

    [[nodiscard]] uint64_t getIntervalSize() const { return intervalSize; }

    static std::expected<TimeInterval, Exception> create(const DatetimeType timeType, const uint64_t intervalSize)
    {
        if (timeType != DatetimeType::MILLISECOND)
        {
            return std::unexpected{NotImplemented("Datetime type {}", magic_enum::enum_name(timeType))};
        }
        return TimeInterval{timeType, intervalSize};
    }
};
class NamedSQLFunction;
template <typename T>
using ConstantEvaluation = std::function<T(const Schema<NamedSQLFunction, Unordered>&)>;
/// TODO Uniform usage of constant functions to pass arguments to remove literal from constants
/// "Constants" Are still evaluated within some scope.
using SQLConstant = VariantContainerFrom<
    ConstantEvaluation,
    std::variant<Literal, LogicalFunction, LogicalOperator, LogicalSource, SinkDescriptor, TimeInterval, UnaryWindowDefinition>>;

class SQLArgFunction;
using SQLFunction = std::variant<SQLConstant, SQLArgFunction>;

class SQLArgFunction
{
    ///For now we only permit "constant" function, i.e. no function objects in SQL
    std::function<SQLFunction(SQLConstant)> function;

    ///Important: Partial application does NOT capture a closure,
    ///instead it builds up a "receiver function" that is only resolved against the context/scope during evaluation
    static SQLFunction applyOneArg(
        std::function<SQLFunction(std::vector<SQLConstant>)> localFunction,
        size_t numArgs,
        std::vector<SQLConstant> appliedArgs,
        SQLConstant currentArg)
    {
        appliedArgs.push_back(std::move(currentArg));
        if (appliedArgs.size() == numArgs)
        {
            return localFunction(std::move(appliedArgs));
        }
        return SQLArgFunction{
            [localFunction = std::move(localFunction), numArgs, appliedArgs = std::move(appliedArgs)](
                SQLConstant nextArg) mutable -> SQLFunction
            { return applyOneArg(std::move(localFunction), numArgs, std::move(appliedArgs), std::move(nextArg)); }};
    }

public:
    explicit SQLArgFunction(std::function<SQLFunction(SQLConstant)> function) : function(std::move(function)) { }

    static SQLFunction create(std::function<SQLFunction(std::vector<SQLConstant>)> function, size_t numArgs)
    {
        auto appliedArgs = std::vector<SQLConstant>{};
        return SQLArgFunction{
            [localFunction = std::move(function), numArgs, appliedArgs = std::move(appliedArgs)](SQLConstant firstArg) mutable
            { return applyOneArg(std::move(localFunction), numArgs, std::move(appliedArgs), std::move(firstArg)); }};
    }
};

class NamedSQLFunction
{
    QualifiedIdentifier name;
    SQLFunction function;

public:
    NamedSQLFunction(QualifiedIdentifier name, SQLFunction function) : name(std::move(name)), function(std::move(function)) { }

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const { return name; }

    [[nodiscard]] const SQLFunction& getFunction() const { return function; }
};

class NestedScope
{
    /// Last scope has the highest priority
    std::vector<Schema<NamedSQLFunction, Unordered>> inheritedScopes;
    Schema<NamedSQLFunction, Unordered> combinedLocalScope;
    Schema<NamedSQLFunction, Unordered> localOnlyScope;
    Schema<NamedSQLFunction, Unordered> inheritableLocalScope;

    NestedScope(
        std::vector<Schema<NamedSQLFunction, Unordered>> inheritedScopes,
        Schema<NamedSQLFunction, Unordered> localOnlyScope,
        Schema<NamedSQLFunction, Unordered> inheritableLocalScope)
        : inheritedScopes(std::move(inheritedScopes))
        , combinedLocalScope(std::array{localOnlyScope, inheritableLocalScope} | std::views::join)
        , localOnlyScope(std::move(localOnlyScope))
        , inheritableLocalScope(std::move(inheritableLocalScope))
    {
    }
public:

    [[nodiscard]] NamedSQLFunction findOrThrow(QualifiedIdentifier name) const
    {
        if (auto found = combinedLocalScope.getFieldByName(name))
        {
            return *found;
        }
        for (const auto& scope : std::views::reverse(inheritedScopes))
        {
            if (auto found = scope.getFieldByName(name))
            {
                return *found;
            }
        }
        /// TODO replace with some symbol not found exception or attach better information for users
        throw FieldNotFound("{}", name);
    }

    static NestedScope create(Schema<NamedSQLFunction, Unordered> rootScope)
    {
        return NestedScope(std::vector{std::move(rootScope)}, {}, {});
    }

    NestedScope createSubScope(Schema<NamedSQLFunction, Unordered> localOnlyScope, Schema<NamedSQLFunction, Unordered> inheritableLocalScope) const
    {
        auto scopeStack = inheritedScopes;
        scopeStack.push_back(inheritableLocalScope);
        return NestedScope{std::move(scopeStack), std::move(localOnlyScope), std::move(inheritableLocalScope)};
    }

};


}

/// NOLINTBEGIN(readability-convert-member-functions-to-static)
class StatementBinder::Impl
{
    Schema<ConfigFieldDefault, Ordered> defaultConfigValues;
    Schema<ConfigFieldTransformation, Unordered> configTransformations;
    Schema<NamedSQLFunction, Unordered> scope;
    std::function<LogicalPlan(AntlrSQLParser::QueryWithSinkContext*)> queryBinder;

public:
    Impl(
        Schema<ConfigFieldDefault, Ordered> defaultConfigValues,
        Schema<ConfigFieldTransformation, Unordered> configTransformations,
        const std::shared_ptr<const SourceCatalog>& sourceCatalog,
        const std::function<LogicalPlan(AntlrSQLParser::QueryWithSinkContext*)>& queryBinder)
        : defaultConfigValues(std::move(defaultConfigValues))
        , configTransformations(std::move(configTransformations))
        , sourceCatalog(sourceCatalog)
        , queryBinder(queryBinder)
    {
    }

    ~Impl() = default;

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
        /// The IDENTIFIER token covers both quoted and unquoted identifiers; the quotation marks are part
        /// of the token text, which is exactly the syntax Identifier::tryParse accepts.
        return bindIdentifier(identifier->getText());
    }

    QualifiedIdentifier bindQualifiedIdentifier(AntlrSQLParser::QualifiedIdentifierContext* qualifiedIdentifier) const
    {
        auto parsed = QualifiedIdentifier::tryParse(qualifiedIdentifier->getText());
        if (not parsed.has_value())
        {
            throw std::move(parsed).error();
        }
        return std::move(parsed).value();
    }

    std::string bindString(std::string_view view) const { return std::string(view.substr(1, view.size() - 2)); }

    std::string bindStringLiteral(AntlrSQLParser::StringLiteralContext* stringLiteral) const
    {
        PRECONDITION(stringLiteral->getText().size() > 1, "String literal must have at least two characters for quotation marks");
        return bindString(stringLiteral->getText());
    }

    std::string bindStringLiteral(antlr4::Token* stringLiteral) const
    {
        PRECONDITION(stringLiteral->getText().size() > 1, "String literal must have at least two characters for quotation marks");
        PRECONDITION(
            stringLiteral->getType() == AntlrSQLParser::STRING_LITERAL, "Attempting to bind a non string token to a string literal");
        return bindString(stringLiteral->getText());
    }

    int64_t bindIntegerLiteral(AntlrSQLParser::SignedIntLiteralContext* signedIntegerLiteral) const
    {
        return from_chars_with_exception<int64_t>(signedIntegerLiteral->getText());
    }

    double bindDoubleLiteral(AntlrSQLParser::FloatLiteralContext* doubleLiteral) const
    {
        return from_chars_with_exception<double>(doubleLiteral->getText());
    }

    bool bindBooleanLiteral(AntlrSQLParser::BoolLiteralContext* booleanLiteral) const { return booleanLiteral->TRUE() != nullptr; }

    Literal bindLiteral(AntlrSQLParser::ScalarLiteralContext* literalAST) const
    {
        if (auto* const stringAST = literalAST->stringLiteral())
        {
            return bindStringLiteral(stringAST);
        }
        if (auto* const signedIntAST = literalAST->signedIntLiteral())
        {
            const auto signedInt = bindIntegerLiteral(signedIntAST);
            if (signedInt >= 0)
            {
                return static_cast<uint64_t>(signedInt);
            }
            return signedInt;
        }
        if (auto* const floatAST = literalAST->floatLiteral())
        {
            return bindDoubleLiteral(floatAST);
        }
        if (auto* const booleanAST = literalAST->boolLiteral())
        {
            return bindBooleanLiteral(booleanAST);
        }
        if (literalAST->null() != nullptr)
        {
            return std::monostate{};
        }
        INVARIANT(false, "Unknown literal type, is the binder out of sync or was a nullptr passed?");
        std::unreachable();
    }

    DataType bindDataType(AntlrSQLParser::TypeDefinitionContext* typeDefAST, const DataType::NULLABLE isNullable) const
    {
        std::string dataTypeText = typeDefAST->getText();

        bool translated = false;
        bool isUnsigned = false;
        if (dataTypeText.starts_with("UNSIGNED "))
        {
            isUnsigned = true;
            translated = true;
            dataTypeText = dataTypeText.substr(std::strlen("UNSIGNED "));
        }

        static const std::unordered_map<std::string, std::string> DataTypeMapping
            = {{"TINYINT", "INT8"}, {"SMALLINT", "INT16"}, {"INT", "INT32"}, {"INTEGER", "INT32"}, {"BIGINT", "INT64"}};

        if (const auto found = DataTypeMapping.find(dataTypeText); found != DataTypeMapping.end())
        {
            translated = true;
            dataTypeText = [&]
            {
                if (isUnsigned)
                {
                    return "U" + found->second;
                }
                return found->second;
            }();
        }
        const auto dataType = DataTypeProvider::tryProvideDataType(dataTypeText, isNullable);
        if (not dataType.has_value())
        {
            if (translated)
            {
                throw UnknownDataType("{}, translated into {}", typeDefAST->getText(), dataTypeText);
            }
            throw UnknownDataType("{}", typeDefAST->getText());
        }
        return *dataType;
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
        return {bindIdentifier(showFilterAST->attr), bindLiteral(showFilterAST->value)};
    }

    std::pair<Identifier, Literal> bindDropFilter(const AntlrSQLParser::DropFilterContext* dropFilterAST) const
    {
        return {bindIdentifier(dropFilterAST->attr), bindLiteral(dropFilterAST->value)};
    }

    /// The options of the SET clause are optional in two layers: the clause itself and the option
    /// sequence between the parentheses may each be absent.
    std::vector<AntlrSQLParser::NamedConfigLiteralContext*> getConfigOptions(AntlrSQLParser::OptionsClauseContext* optionsClause) const
    {
        if (optionsClause == nullptr || optionsClause->options == nullptr)
        {
            return {};
        }
        return optionsClause->options->namedConfigLiteral();
    }

    std::variant<Literal, Schema<UnqualifiedUnboundField, Ordered>> bindConfigValue(AntlrSQLParser::LiteralContext* literalAST) const
    {
        if (auto* const scalarAST = literalAST->scalarLiteral())
        {
            return bindLiteral(scalarAST);
        }
        if (auto* const schemaCtorAST = literalAST->schemaCtor())
        {
            return bindSchema(schemaCtorAST->schemaDefinition());
        }
        INVARIANT(false, "Unknown config literal type, is the binder out of sync or was a nullptr passed?");
        std::unreachable();
    }

    Schema<LiteralConfigValue, Ordered> bindConfigValues(const std::vector<AntlrSQLParser::NamedConfigLiteralContext*>& configOptions) const
    {
        std::vector<LiteralConfigValue> options{};
        for (auto* const configOption : configOptions)
        {
            auto configKey = bindQualifiedIdentifier(configOption->qualifiedIdentifier());
            auto value = bindConfigValue(configOption->literal());
            ConfigLiteral literalValue{std::visit(
                Overloaded{
                    [](Literal simpleLiteralVariant) -> ConfigLiteral
                    {
                        return std::visit(
                            Overloaded{
                                [](const uint64_t unsignedInt)
                                {
                                    if (not std::in_range<int64_t>(unsignedInt))
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

    /// TODO #764 use identifier lists instead of map of maps
    ConfigMap bindConfigOptions(const std::vector<AntlrSQLParser::NamedConfigLiteralContext*>& configOptions) const
    {
        ConfigMap boundConfigOptions{};
        for (auto* const configOption : configOptions)
        {
            const auto identifiers = configOption->qualifiedIdentifier()->IDENTIFIER();
            if (identifiers.size() != 2)
            {
                throw InvalidConfigParameter(
                    "Config key needs to be qualified exactly once, but was {}", configOption->qualifiedIdentifier()->getText());
            }
            const auto rootIdentifier = bindIdentifier(identifiers.at(0)->getText());
            auto optionName = bindIdentifier(identifiers.at(1)->getText());
            boundConfigOptions.try_emplace(
                rootIdentifier, std::unordered_map<Identifier, std::variant<Literal, Schema<UnqualifiedUnboundField, Ordered>>>{});

            if (not boundConfigOptions.at(rootIdentifier).try_emplace(optionName, bindConfigValue(configOption->literal())).second)
            {
                throw InvalidConfigParameter("Duplicate option for source: {}", configOption->qualifiedIdentifier()->getText());
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
                bindQualifiedIdentifier(configOption->qualifiedIdentifier()), bindConfigValue(configOption->literal()));
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

        /// Ugly but necesarry peak into the config values since the config schema of the input formatter depends on the type of the input formatter
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
                  .transform([&](const SourceConfigSchema& configSchema)
                             { return configSchema.withConfigDefaults(configDefaults).withConfigTransformations(configTransformations); });

        return unwrapOrThrow(
            configSchema.and_then([&](const SourceConfigSchema& configSchema) { return configSchema.resolveConfigs(configOptions); }));
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
            if (not typeLiteral.has_value())
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
        /// TODO #764 use normal identifiers for types
        const Identifier sourceType = bindIdentifier(physicalSourceDefAST->type);
        const auto validatedConfig = bindSourceConfig(
            sourceType, getConfigOptions(physicalSourceDefAST->optionsClause()), defaultConfigValues, configTransformations);

        const auto& [generalConfig, pluginConfig, inputFormatter, schemaOpt] = validatedConfig;
        if (schemaOpt.has_value())
        {
            throw InvalidQuerySyntax("Schema cannot be specified for physical source attached to logical source");
        }
        return CreatePhysicalSourceStatement{std::move(logicalSourceName), generalConfig, pluginConfig, inputFormatter};
    }

    CreateWorkerStatement bindCreateWorkerStatement(AntlrSQLParser::CreateWorkerDefinitionContext* workerDefAST) const
    {
        auto configs = bindConfigOptionsWithDuplicates(getConfigOptions(workerDefAST->optionsClause()));

        auto capacity = [&] -> std::optional<size_t>
        {
            auto it = std::ranges::find_if(
                configs,
                [](const auto& key) { return key.first.size() == 1 && *std::ranges::begin(key.first) == Identifier::parse("CAPACITY"); });
            if (it != configs.end())
            {
                auto* literalOpt = std::get_if<Literal>(&it->second);
                if (literalOpt && std::holds_alternative<uint64_t>(*literalOpt))
                {
                    return static_cast<size_t>(std::get<uint64_t>(*literalOpt));
                }
                throw InvalidQuerySyntax("Capacity must be an unsigned integer literal");
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
                if (literalOpt && std::holds_alternative<std::string>(*literalOpt))
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
                           if (literalOpt && std::holds_alternative<std::string>(*literalOpt))
                           {
                               return URI(std::get<std::string>(*literalOpt)).toString();
                           }
                           throw InvalidQuerySyntax("DOWNSTREAM must be a string literal");
                       })
                | std::ranges::to<std::vector<std::string>>();
        }();


        return CreateWorkerStatement{
            .host = URI(bindStringLiteral(workerDefAST->hostaddr)).toString(),
            .dataAddress = std::move(dataAddress),
            .capacity = capacity,
            .downstream = downStreams,
            .config = {}};
    }

    CreateSinkStatement bindCreateSinkStatement(AntlrSQLParser::CreateSinkDefinitionContext* sinkDefAST) const
    {
        const auto sinkName = bindIdentifier(sinkDefAST->sinkName);
        /// TODO #764 use normal identifiers for types
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
        auto inputs = bindSchema(modelDefAST->inputSchema);
        auto outputs = bindSchema(modelDefAST->outputSchema);
        /// Inference does not support nullable tensors
        const auto requireNonNullable = [](const Schema<UnqualifiedUnboundField, Ordered>& schema, const std::string_view schemaKind)
        {
            for (const auto& field : schema)
            {
                if (field.getDataType().nullable)
                {
                    throw InvalidQuerySyntax("Model {} field {} must be declared NOT NULL", schemaKind, field.getFullyQualifiedName());
                }
            }
        };
        requireNonNullable(inputs, "input");
        requireNonNullable(outputs, "output");
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
            if (not std::holds_alternative<std::string>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW LOGICAL SOURCES must be a string");
            }
            return ShowLogicalSourcesStatement{.name = bindIdentifier(std::get<std::string>(value)), .format = format};
        }
        return ShowLogicalSourcesStatement{.name = std::nullopt, .format = format};
    }

    ShowPhysicalSourcesStatement bindShowPhysicalSourcesStatement(
        const AntlrSQLParser::ShowFilterContext* showFilter, const std::optional<StatementOutputFormat>& format) const
    {
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            static const auto IdIdentifier = Identifier::parse("ID");
            if (attr != IdIdentifier)
            {
                throw InvalidQuerySyntax("Filter for SHOW PHYSICAL SOURCES must be on id attribute");
            }
            if (not std::holds_alternative<uint64_t>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW PHYSICAL SOURCES must be an unsigned integer");
            }
            return ShowPhysicalSourcesStatement{.logicalSource = std::nullopt, .id = std::get<uint64_t>(value), .format = format};
        }
        return ShowPhysicalSourcesStatement{.logicalSource = std::nullopt, .id = std::nullopt, .format = format};
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
            if (not std::holds_alternative<std::string>(value))
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
            if (not std::holds_alternative<std::string>(value))
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
        /// The format is a plain identifier in the grammar; it is validated here in the binder.
        const std::optional<StatementOutputFormat> format
            = showAST->identifier() != nullptr ? std::make_optional(bindFormat(bindIdentifier(showAST->identifier()))) : std::nullopt;
        auto* const subject = showAST->showSubject();

        if (subject->showLogicalSourcesSubject() != nullptr)
        {
            return bindShowLogicalSourcesStatement(showFilter, format);
        }
        if (subject->showPhysicalSourcesSubject() != nullptr)
        {
            return bindShowPhysicalSourcesStatement(showFilter, format);
        }
        if (subject->showQueriesSubject() != nullptr)
        {
            return bindShowQueriesStatement(showFilter, format);
        }
        if (subject->showSinksSubject() != nullptr)
        {
            return bindShowSinksStatement(showFilter, format);
        }
        if (subject->showModelsSubject() != nullptr)
        {
            return ShowModelsStatement{.format = format};
        }
        throw InvalidStatement("Unrecognized SHOW statement");
    }

    /// Validate a filter and extract its typed value, or throw a syntax error tied to `subject`.
    template <typename T>
    T requireFilterValue(
        const std::pair<Identifier, Literal>& filter,
        std::string_view expectedAttr,
        std::string_view expectedTypeDescription,
        std::string_view subject) const
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
        if (const auto physicalSource = sourceCatalog->getPhysicalSource(PhysicalSourceId{id}))
        {
            return DropPhysicalSourceStatement{*physicalSource};
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
            throw InvalidStatement("DROP WORKER statements are not implemented yet");
        }
        throw InvalidStatement("Unrecognized DROP statement");
    }

    std::unordered_set<ExplainStage> bindExplainStages(AntlrSQLParser::ExplainStagesContext* stagesContext) const
    {
        std::unordered_set<ExplainStage> explainStages;

        for (auto* const stageContext : stagesContext->explainStage())
        {
            if (stageContext->LOGICAL() != nullptr)
            {
                if (!explainStages.insert(ExplainStage::Logical).second)
                {
                    throw InvalidStatement("Duplicate EXPLAIN stage: LOGICAL");
                }
            }
            else
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
        }

        return explainStages;
    }

    ExplainFormat bindExplainFormat(AntlrSQLParser::ExplainFormatContext* formatAST) const
    {
        if (formatAST->TEXT() != nullptr)
        {
            return ExplainFormat::Text;
        }

        auto parsedFormatIdentifier = bindIdentifier(formatAST->identifier());

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

        return ExplainQueryStatement{.plan = queryBinder(explainAst->queryWithSink()), .explainFormat = format, .explainStages = stages};
    }

    TypedLogicalOperator<SinkLogicalOperator> bindSink(AntlrSQLParser::IntoTermContext* sinkAst, LogicalOperator lastOp) const { }

    LogicalOperator bindOpenRelationalExpression(AntlrSQLParser::OpenRelationalExpressionContext* openRelExpression) const { }

    TypedLogicalOperator<ProjectionLogicalOperator>
    bindProjection(const std::vector<AntlrSQLParser::ProjectionContext*>& projections, LogicalOperator input) const
    {
    }

    TypedLogicalOperator<SelectionLogicalOperator>
    bindSelection(AntlrSQLParser::OpenScalarExpressionContext* filterExpression, LogicalOperator input) const
    {
    }

    LogicalFunction
    bindOpenScalarExpression(AntlrSQLParser::OpenScalarExpressionContext* openScalarExpression, const LogicalOperator& input) const
    {
    }

    DatetimeType bindDatetimeType(AntlrSQLParser::DatetimeFieldContext* datetimeType) const
    {
        if (datetimeType->YEAR() != nullptr)
        {
            return DatetimeType::YEAR;
        }
        if (datetimeType->MONTH() != nullptr)
        {
            return DatetimeType::MONTH;
        }
        if (datetimeType->DAY() != nullptr)
        {
            return DatetimeType::DAY;
        }
        if (datetimeType->HOUR() != nullptr)
        {
            return DatetimeType::HOUR;
        }
        if (datetimeType->MINUTE() != nullptr)
        {
            return DatetimeType::MINUTE;
        }
        if (datetimeType->SECOND() != nullptr)
        {
            return DatetimeType::SECOND;
        }
        if (datetimeType->MILLISECOND() != nullptr)
        {
            return DatetimeType::MILLISECOND;
        }
        NES_ERROR("Fell through all datetime type alternatives, are the binder and the grammar in sync?");
        std::unreachable();
    }

    TimeInterval bindIntervalExpression(AntlrSQLParser::IntervalExpressionContext* intervalExpression, const LogicalOperator& input) const
    {
        INVARIANT(intervalExpression->openScalarExpression() != nullptr, "Should be enforced by antlr");
        auto scalarExpression = bindOpenScalarExpression(intervalExpression->openScalarExpression(), input);
        if (auto constant = scalarExpression.tryGetAs<ConstantValueLogicalFunction>())
        {
            if ((*constant)->getDataType().isInteger() && !(*constant)->getDataType().isSignedInteger())
            {
                const auto intOpt = from_chars<uint64_t>((*constant)->getConstantValue());
                if (!intOpt.has_value())
                {
                    throw InvalidQuerySyntax(
                        "Something went wrong when trying to interpret \"{}\" as an uint64_t constant as an argument for an interval "
                        "expression");
                }
                const auto intervalType = bindDatetimeType(intervalExpression->datetimeField());
                return unwrapOrThrow(TimeInterval::create(intervalType, intOpt.value()));
            }
            throw NotImplemented("Creating an interval expression from non-unsigned integer literals of type {}", constant->getDataType());
        }
        throw NotImplemented("Creating an interval expression from a non-literal expression {}", intervalExpression->getText());
    }

    ConstantEvaluation<UnaryWindowDefinition>
    bindUnaryWindow(AntlrSQLParser::WindowTermContext* windowTerm) const
    {
        auto tumblingFunction = [](std::vector<SQLConstant> arguments)
        {
            return ConstantEvaluation<UnaryWindowDefinition>{[arguments = std::move(arguments)]
            {
                if (arguments.size())
            }}
        }
        if (windowTerm->TUMBLING != nullptr)
        {
            return [](TimeInterval timeInterval)
            {
                INVARIANT(timeInterval.getTimeType() == DatetimeType::MILLISECOND, "Windows must be created from milliseconds");
                return Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{timeInterval.getIntervalSize()}}};
            };
        }
        if (windowType->SLIDING() != nullptr)
        {
            return [](TimeInterval timeInterval)
            {
                INVARIANT(timeInterval.getTimeType() == DatetimeType::MILLISECOND, "Windows must be created from milliseconds");
                return Windowing::TimeBasedWindowType
                {
                    Windowing::SlidingWindow
                    {
                    }
                }
            }
        }
    }

    std::function<TypedLogicalOperator<WindowedAggregationLogicalOperator>(
        LogicalOperator,
        const std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation>&,
        const std::vector<std::pair<Identifier, FieldAccessLogicalFunction>>&)>
    bindWindowedAggregation(AntlrSQLParser::GroupByTermContext* groupByAst) const
    {
        INVARIANT(groupByAst->unaryWindowLiteral() != nullptr, "Should be enforced by antlr");
        return [=, this](
                   LogicalOperator input,
                   const std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation>& aggFunctions,
                   const std::vector<std::pair<Identifier, TypedLogicalFunction<FieldAccessLogicalFunction>>>& namedKeys)
                   -> TypedLogicalOperator<WindowedAggregationLogicalOperator>
        {
            auto flippedKeys = namedKeys
                | std::views::transform([](const std::pair<Identifier, TypedLogicalFunction<FieldAccessLogicalFunction>>& pair)
                                        { return std::pair{pair.second, std::optional{pair.first}}; })
                | std::ranges::to<std::vector>();

            auto tsFieldId = bindIdentifier(groupByAst->unaryWindowLiteral()->IDENTIFIER()->getText());
            auto tsField = [&]
            {
                if (auto foundField = input->getOutputSchema().getFieldByName(tsFieldId))
                {
                    return std::move(foundField).value();
                }
                throw FieldNotFound("{}", tsFieldId);
            }();

            auto interval = bindIntervalExpression(groupByAst->unaryWindowLiteral()->intervalExpression(), input);


            return WindowedAggregationLogicalOperator::create(std::move(flippedKeys), aggFunctions, )
        };
    }

    TypedLogicalOperator<ProjectionLogicalOperator> bindQuery(AntlrSQLParser::QueryContext* queryAst) const
    {
        INVARIANT(queryAst->fromTerm() != nullptr, "Should be enforced by antlr");
        INVARIANT(queryAst->fromTerm()->openRelationalExpression() != nullptr, "Should be enforced by antlr");
        INVARIANT(queryAst->projectionTerm() != nullptr, "Should be enforced by antlr");
        INVARIANT(queryAst->projectionTerm()->projectionSeq() != nullptr, "Should be enforced by antlr");

        auto from = bindOpenRelationalExpression(queryAst->fromTerm()->openRelationalExpression());
        if (queryAst->filterTerm() != nullptr)
        {
            if (queryAst->filterTerm()->openScalarExpression() != nullptr)
            {
                from = bindSelection(queryAst->filterTerm()->openScalarExpression(), std::move(from));
            }
            else
            {
                throw InvalidQuerySyntax("WHERE must be followed by a filter expression but wasn't");
            }
        }
        if (queryAst->groupByTerm() != nullptr)
        {
        }
    }

    TypedLogicalOperator<UnionLogicalOperator> bindUnionSeq(AntlrSQLParser::UnionSeqContext* unionSeq) const
    {
        auto unionChilds = unionSeq->query()
            | std::views::transform([this](AntlrSQLParser::QueryContext* queryAst) -> LogicalOperator { return bindQuery(queryAst); })
            | std::ranges::to<std::vector>();
        return UnionLogicalOperator::create(std::move(unionChilds));
    }

    TypedLogicalOperator<SinkLogicalOperator> bindQueryWithSink(AntlrSQLParser::QueryWithSinkContext* queryAst) const
    {
        INVARIANT(queryAst->unionSeq() != nullptr, "Should be enforced by antlr");
        INVARIANT(queryAst->intoTerm() != nullptr, "Should be enforced by antlr");
        auto lastOp = bindUnionSeq(queryAst->unionSeq());
        return bindSink(queryAst->intoTerm(), std::move(lastOp));
    }

    std::expected<Statement, Exception> bind(AntlrSQLParser::StatementContext* statementAST) const
    {
        try
        {
            if (auto* const createAST = statementAST->createStatement(); createAST != nullptr)
            {
                return bindCreateStatement(createAST);
            }
            if (auto* showAST = statementAST->showStatement(); showAST != nullptr)
            {
                return bindShowStatement(showAST);
            }
            if (auto* dropAst = statementAST->dropStatement(); dropAst != nullptr)
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
                            auto* literal = std::get_if<Literal>(&idIter->second);
                            if ((literal == nullptr) || !std::holds_alternative<std::string>(*literal))
                            {
                                throw InvalidQuerySyntax("Query id must be a string");
                            }
                            queryId = DistributedQueryId(std::get<std::string>(*literal));
                        }
                    }
                }
                return QueryStatement{.plan = queryBinder(queryAst->queryWithSink()), .id = queryId};
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
    Schema<ConfigFieldDefault, Ordered> defaultConfigValues,
    Schema<ConfigFieldTransformation, Unordered> configTransformations,
    const std::shared_ptr<const SourceCatalog>& sourceCatalog,
    const std::function<LogicalPlan(AntlrSQLParser::QueryWithSinkContext*)>& queryPlanBinder)
    : impl(std::make_unique<Impl>(std::move(defaultConfigValues), std::move(configTransformations), sourceCatalog, queryPlanBinder))
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

std::expected<std::vector<std::expected<Statement, Exception>>, Exception>
StatementBinder::parseAndBind(const std::string_view statementString) const
{
    try
    {
        antlr4::ANTLRInputStream input(statementString.data(), statementString.length());
        AntlrSQLLexer lexer(&input);
        antlr4::CommonTokenStream tokens(&lexer);
        AntlrSQLParser parser(&tokens);
        /// Remove default error listeners that print to stdout/stderr
        lexer.removeErrorListeners();
        parser.removeErrorListeners();
        /// Enable that antlr throws exceptions on parsing errors
        parser.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());
        AntlrSQLParser::MultipleStatementsContext* tree = parser.multipleStatements();
        if (tree == nullptr)
        {
            return std::unexpected{InvalidQuerySyntax("{}", statementString)};
        }

        return std::expected<std::vector<std::expected<Statement, Exception>>, Exception>{
            tree->statement() | std::views::transform([this](auto* statementAST) { return impl->bind(statementAST); })
            | std::ranges::to<std::vector>()};
    }
    catch (antlr4::ParseCancellationException& e)
    {
        return std::unexpected{InvalidQuerySyntax("{}", e)};
    }
}

std::expected<Statement, Exception> StatementBinder::parseAndBindSingle(std::string_view statementStrings) const
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
