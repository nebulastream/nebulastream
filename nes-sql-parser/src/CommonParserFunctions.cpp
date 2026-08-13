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


#include <CommonParserFunctions.hpp>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Sinks/SinkConfigSchema.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceConfigSchema.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Variant.hpp>
#include <InputFormatterDescriptor.hpp>

#include <AntlrSQLParser.h>
#include <Configurations/ConfigLiteral.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
/// The formatter type decides which formatter options exist, so this reads it before the options are resolved.
Identifier bindFormatterType(
    const QualifiedIdentifier& typeOption,
    const Schema<LiteralConfigValue, Ordered>& options,
    const Schema<ConfigFieldDefault, Ordered>& defaults)
{
    /// Compares whole identifiers, because a schema lookup also matches a suffix of a qualified one.
    const auto countOf = [&options](const QualifiedIdentifier& option)
    { return std::ranges::count(options, option, &LiteralConfigValue::getFullyQualifiedName); };
    if (countOf(QualifiedIdentifier::parse("TYPE")) > 0)
    {
        throw InvalidConfigParameter("Unqualified TYPE parameter is potentially ambiguous, write {} instead", typeOption);
    }
    if (countOf(typeOption) > 1)
    {
        throw InvalidConfigParameter("{} is set more than once", typeOption);
    }
    auto literal = options.getFieldByName(typeOption).transform([](const LiteralConfigValue& option) { return option.getValue(); });
    if (not literal.has_value())
    {
        literal
            = defaults.getFieldByName(typeOption).transform([](const ConfigFieldDefault& configDefault) { return configDefault.get(); });
    }
    if (not literal.has_value())
    {
        throw InvalidQuerySyntax("{} must be specified", typeOption);
    }
    return unwrapOrThrow(tryGetOr<std::string>(*literal, expectedType<std::string>()).and_then(Identifier::tryParse));
}
}

Identifier bindIdentifier(AntlrSQLParser::IdentifierContext* identifier)
{
    return bindIdentifier(identifier->strictIdentifier());
}

Identifier bindIdentifier(AntlrSQLParser::StrictIdentifierContext* strictIdentifier)
{
    const auto idOpt = [&strictIdentifier]
    {
        if (auto* const unquotedIdentifier = dynamic_cast<AntlrSQLParser::UnquotedIdentifierContext*>(strictIdentifier))
        {
            return Identifier::tryParse(unquotedIdentifier->getText());
        }
        if (auto* const quotedIdentifier = dynamic_cast<AntlrSQLParser::QuotedIdentifierAlternativeContext*>(strictIdentifier))
        {
            const auto withQuotationMarks = quotedIdentifier->quotedIdentifier()->BACKQUOTED_IDENTIFIER()->getText();
            auto withoutMarks = withQuotationMarks.substr(1, withQuotationMarks.size() - 2);
            return Identifier::tryParse(fmt::format("\"{}\"", withoutMarks));
        }
        INVARIANT(
            false,
            "Unknown identifier type, was neither valid quoted or unquoted, is the grammar out of sync with the binder or was a nullptr "
            "passed?");
        std::unreachable();
    }();
    if (not idOpt.has_value())
    {
        /// NOLINTNEXTLINE(bugprone-exception-copy-constructor-throws) NES exceptions carry strings
        throw std::move(idOpt).error();
    }
    return idOpt.value();
}

std::
    tuple<GeneralSourceConfig, PluginSourceConfiguration, InputFormatterDescriptor, std::optional<Schema<UnqualifiedUnboundField, Ordered>>>
    bindSourceConfig(
        const Identifier& sourceType,
        const std::vector<AntlrSQLParser::NamedConfigExpressionContext*>& configOptionsAst,
        const Schema<ConfigFieldDefault, Ordered>& configDefaults)
{
    const auto configOptions = bindConfigValues(configOptionsAst);
    const auto inputFormatterType = bindFormatterType(QualifiedIdentifier::parse("INPUT_FORMATTER.TYPE"), configOptions, configDefaults);

    auto configSchema = SourceConfigSchema::create(sourceType, inputFormatterType)
                            .transform([&](const SourceConfigSchema& schema) { return schema.withConfigDefaults(configDefaults); });

    return unwrapOrThrow(configSchema.and_then([&](const SourceConfigSchema& schema) { return schema.resolveConfigs(configOptions); }));
}

std::tuple<GeneralSinkConfig, AnonymousSinkSchema, PluginSinkConfiguration, OutputFormatterDescriptor> bindSinkConfig(
    const Identifier& sinkType,
    const std::vector<AntlrSQLParser::NamedConfigExpressionContext*>& configOptionsAst,
    const Schema<ConfigFieldDefault, Ordered>& configDefaults)
{
    const auto configOptions = bindConfigValues(configOptionsAst);
    const auto outputFormatterType = bindFormatterType(QualifiedIdentifier::parse("OUTPUT_FORMATTER.TYPE"), configOptions, configDefaults);

    auto configSchema = SinkConfigSchema::create(sinkType, outputFormatterType)
                            .transform([&](const SinkConfigSchema& schema) { return schema.withConfigDefaults(configDefaults); });
    return unwrapOrThrow(configSchema.and_then([&](const SinkConfigSchema& schema) { return schema.resolveConfigs(configOptions); }));
}

QualifiedIdentifier bindQualifiedIdentifier(AntlrSQLParser::IdentifierChainContext* identifierList)
{
    return identifierList->strictIdentifier()
        | std::views::transform([](AntlrSQLParser::StrictIdentifierContext* identifier) { return bindIdentifier(identifier); })
        | std::ranges::to<QualifiedIdentifier>();
}

Identifier bindIdentifier(std::string identifier)
{
    auto identifierExpected = Identifier::tryParse(std::move(identifier));
    if (!identifierExpected.has_value())
    {
        throw std::move(identifierExpected).error();
    }
    return identifierExpected.value();
}

Schema<LiteralConfigValue, Ordered> bindConfigValues(const std::vector<AntlrSQLParser::NamedConfigExpressionContext*>& configOptions)
{
    std::vector<LiteralConfigValue> options{};
    for (auto* const configOption : configOptions)
    {
        auto configKey = bindQualifiedIdentifier(configOption->identifierChain());
        std::variant<Literal, Schema<UnqualifiedUnboundField, Ordered>> value{};

        if (configOption->constant() != nullptr)
        {
            value = bindLiteral(configOption->constant());
        }
        else if (configOption->schema() != nullptr)
        {
            value = bindSchema(configOption->schema()->schemaDefinition());
        }
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
                                    throw InvalidQuerySyntax("Integer literal {} exceeds the supported signed integer range", unsignedInt);
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
ConfigMap bindConfigOptions(const std::vector<AntlrSQLParser::NamedConfigExpressionContext*>& configOptions)
{
    ConfigMap boundConfigOptions{};
    for (auto* const configOption : configOptions)
    {
        if (configOption->name->strictIdentifier().size() != 2)
        {
            throw InvalidConfigParameter("Config key needs to be qualified exactly once, but was {}", configOption->name->getText());
        }
        const auto rootIdentifier = bindIdentifier(configOption->name->strictIdentifier().at(0));
        auto optionName = bindIdentifier(configOption->name->strictIdentifier().at(1));
        boundConfigOptions.try_emplace(
            rootIdentifier, std::unordered_map<Identifier, std::variant<Literal, Schema<UnqualifiedUnboundField, Ordered>>>{});

        std::variant<Literal, Schema<UnqualifiedUnboundField, Ordered>> value{};

        if (configOption->constant() != nullptr)
        {
            value = bindLiteral(configOption->constant());
        }
        else if (configOption->schema() != nullptr)
        {
            value = bindSchema(configOption->schema()->schemaDefinition());
        }

        if (not boundConfigOptions.at(rootIdentifier).try_emplace(optionName, value).second)
        {
            throw InvalidConfigParameter("Duplicate option for source: {}", configOption->name->getText());
        }
    }
    return boundConfigOptions;
}

ConfigMultiMap bindConfigOptionsWithDuplicates(const std::vector<AntlrSQLParser::NamedConfigExpressionContext*>& configOptions)
{
    ConfigMultiMap boundConfigOptions;
    for (auto* const configOption : configOptions)
    {
        auto pathExp = QualifiedIdentifier::tryParse(configOption->name->getText());
        if (not pathExp.has_value())
        {
            throw std::move(pathExp).error();
        }

        std::variant<Literal, Schema<UnqualifiedUnboundField, Ordered>> value{};
        if (configOption->constant() != nullptr)
        {
            value = bindLiteral(configOption->constant());
        }
        else if (configOption->schema() != nullptr)
        {
            value = bindSchema(configOption->schema()->schemaDefinition());
        }
        boundConfigOptions.emplace_back(std::move(pathExp).value(), value);
    }
    return boundConfigOptions;
}

namespace
{
std::string bindString(std::string_view view)
{
    return std::string(view.substr(1, view.size() - 2));
}
}

std::string bindStringLiteral(AntlrSQLParser::StringLiteralContext* stringLiteral)
{
    PRECONDITION(stringLiteral->getText().size() > 1, "String literal must have at least two characters for quotation marks");
    return bindString(stringLiteral->getText());
}

std::string bindStringLiteral(antlr4::Token* stringLiteral)
{
    PRECONDITION(stringLiteral->getText().size() > 1, "String literal must have at least two characters for quotation marks");
    PRECONDITION(stringLiteral->getType() == AntlrSQLParser::STRING, "Attempting to bind a non string token to a string literal");
    return bindString(stringLiteral->getText());
}

int64_t bindIntegerLiteral(AntlrSQLParser::IntegerLiteralContext* integerLiteral)
{
    return from_chars_with_exception<int64_t>(integerLiteral->getText());
}

int64_t bindIntegerLiteral(AntlrSQLParser::SignedIntegerLiteralContext* signedIntegerLiteral)
{
    return from_chars_with_exception<int64_t>(signedIntegerLiteral->getText());
}

uint64_t bindUnsignedIntegerLiteral(AntlrSQLParser::UnsignedIntegerLiteralContext* unsignedIntegerLiteral)
{
    return from_chars_with_exception<uint64_t>(unsignedIntegerLiteral->getText());
}

double bindDoubleLiteral(AntlrSQLParser::FloatLiteralContext* doubleLiteral)
{
    return from_chars_with_exception<double>(doubleLiteral->getText());
}

bool bindBooleanLiteral(AntlrSQLParser::BooleanLiteralContext* booleanLiteral)
{
    return from_chars_with_exception<bool>(booleanLiteral->getText());
}

Literal bindLiteral(AntlrSQLParser::ConstantContext* literalAST)
{
    if (auto* const stringAST = dynamic_cast<AntlrSQLParser::StringLiteralContext*>(literalAST))
    {
        return bindStringLiteral(stringAST);
    }
    if (auto* const numericLiteral = dynamic_cast<AntlrSQLParser::NumericLiteralContext*>(literalAST))
    {
        if (auto* const intLocation = dynamic_cast<AntlrSQLParser::IntegerLiteralContext*>(numericLiteral->number()))
        {
            const auto signedInt = bindIntegerLiteral(intLocation);
            if (signedInt >= 0)
            {
                return static_cast<uint64_t>(signedInt);
            }
            return signedInt;
        }
        if (auto* const doubleLocation = dynamic_cast<AntlrSQLParser::FloatLiteralContext*>(numericLiteral->number()))
        {
            return bindDoubleLiteral(doubleLocation);
        }
    }
    if (auto* const booleanLocation = dynamic_cast<AntlrSQLParser::BooleanLiteralContext*>(literalAST))
    {
        return bindBooleanLiteral(booleanLocation);
    }
    if (dynamic_cast<AntlrSQLParser::NullLiteralContext*>(literalAST) != nullptr)
    {
        return std::monostate{};
    }
    if (dynamic_cast<AntlrSQLParser::TypeConstructorContext*>(literalAST) != nullptr)
    {
        throw InvalidQuerySyntax("Typed constant '{}' is not supported here", literalAST->getText());
    }
    INVARIANT(false, "Unknown literal type, is the binder out of sync or was a nullptr passed?");
    std::unreachable();
}

std::pair<Identifier, Literal> bindShowFilter(const AntlrSQLParser::ShowFilterContext* showFilterAST)
{
    return {bindIdentifier(showFilterAST->attr), bindLiteral(showFilterAST->value)};
}

std::pair<Identifier, Literal> bindDropFilter(const AntlrSQLParser::DropFilterContext* dropFilterAST)
{
    return {bindIdentifier(dropFilterAST->attr), bindLiteral(dropFilterAST->value)};
}

Schema<UnqualifiedUnboundField, Ordered> bindSchema(AntlrSQLParser::SchemaDefinitionContext* schemaDefAST)
{
    std::vector<UnqualifiedUnboundField> fields{};

    for (auto* const column : schemaDefAST->columnDefinition())
    {
        auto isNullableBool = column->nullableDefinition() == nullptr || !(not column->nullableDefinition()->getText().empty());
        auto isNullable = isNullableBool ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
        auto dataType = bindDataType(column->typeDefinition(), isNullable);
        auto columnName = bindIdentifier(column->strictIdentifier());
        fields.emplace_back(columnName, dataType);
    }
    const auto boundSchema = Schema<UnqualifiedUnboundField, Ordered>::tryCreateCollisionFree(std::move(fields));
    if (!boundSchema.has_value())
    {
        throw FieldAlreadyExists(Schema<UnqualifiedUnboundField, Ordered>::createCollisionString(boundSchema.error()));
    }
    return boundSchema.value();
}

DataType bindDataType(AntlrSQLParser::TypeDefinitionContext* typeDefAST, const DataType::NULLABLE isNullable)
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

[[nodiscard]] std::string literalToString(const Literal& literal)
{
    return std::visit(
        Overloaded{
            [](std::string string) { return string; },
            [](int64_t integer) { return std::to_string(integer); },
            [](uint64_t unsignedInteger) { return std::to_string(unsignedInteger); },
            [](const double doubleLiteral) { return std::to_string(doubleLiteral); },
            [](const bool boolean) -> std::string { return boolean ? "true" : "false"; },
            [](std::monostate) -> std::string { return "NULL"; }},
        literal);
}
}
