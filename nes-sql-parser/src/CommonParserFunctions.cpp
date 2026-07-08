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

#include <cctype>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <AntlrSQLParser.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{
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
        throw std::move(idOpt).error();
    }
    return idOpt.value();
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
/// Converts a config option entry to a lowercase string key-value pair.
/// Returns std::nullopt for non-Literal values (e.g., Schema), which are handled by separate functions.
std::optional<std::pair<Identifier, std::string>>
configOptionToValue(const std::pair<const Identifier, std::variant<Literal, Schema<UnqualifiedUnboundField, Ordered>>>& entry)
{
    if (!std::holds_alternative<Literal>(entry.second))
    {
        return std::nullopt;
    }
    const auto value = literalToString(std::get<Literal>(entry.second));
    return std::make_pair(entry.first, value);
}

/// Collects all literal config options that live under any of the given top-level prefixes into a single key-value map.
std::unordered_map<Identifier, std::string>
collectConfigBlock(const ConfigMap& configOptions, const std::initializer_list<std::string_view> prefixes)
{
    auto config = std::unordered_map<Identifier, std::string>{};
    for (const auto prefix : prefixes)
    {
        if (const auto configIter = configOptions.find(Identifier::parse(std::string{prefix})); configIter != configOptions.end())
        {
            for (const auto& entry : configIter->second | std::views::transform(configOptionToValue))
            {
                if (entry.has_value())
                {
                    config.insert_or_assign(entry->first, entry->second);
                }
            }
        }
    }
    return config;
}
} /// namespace

std::unordered_map<Identifier, std::string> parseInputFormatterConfig(const ConfigMap& configOptions)
{
    return collectConfigBlock(configOptions, {"INPUT_FORMATTER"});
}

std::unordered_map<Identifier, std::string> parseOutputFormatterConfig(const ConfigMap& configOptions)
{
    return collectConfigBlock(configOptions, {"OUTPUT_FORMATTER"});
}

std::unordered_map<Identifier, std::string> getSourceConfig(const ConfigMap& configOptions)
{
    std::unordered_map<Identifier, std::string> sourceOptions{};
    if (const auto sourceConfigIter = configOptions.find(Identifier::parse("SOURCE")); sourceConfigIter != configOptions.end())
    {
        sourceOptions = sourceConfigIter->second | std::views::transform(configOptionToValue)
            | std::views::filter([](const auto& opt) { return opt.has_value(); })
            | std::views::transform([](const auto& opt) { return *opt; }) | std::ranges::to<std::unordered_map<Identifier, std::string>>();
    }

    return sourceOptions;
}

std::unordered_map<Identifier, std::string> getSinkConfig(const ConfigMap& configOptions)
{
    std::unordered_map<Identifier, std::string> sinkOptions{};
    if (const auto sourceConfigIter = configOptions.find(Identifier::parse("SINK")); sourceConfigIter != configOptions.end())
    {
        sinkOptions = sourceConfigIter->second | std::views::transform(configOptionToValue)
            | std::views::filter([](const auto& opt) { return opt.has_value(); })
            | std::views::transform([](const auto& opt) { return *opt; }) | std::ranges::to<std::unordered_map<Identifier, std::string>>();
    }

    return sinkOptions;
}

namespace
{
std::optional<Schema<UnqualifiedUnboundField, Ordered>> getSchema(ConfigMap configOptions, const Identifier& configName)
{
    if (const auto sourceConfigIter = configOptions.find(configName); sourceConfigIter != configOptions.end())
    {
        if (const auto schemaIter = sourceConfigIter->second.find(Identifier::parse("SCHEMA"));
            schemaIter != sourceConfigIter->second.end())
        {
            if (std::holds_alternative<Schema<UnqualifiedUnboundField, Ordered>>(schemaIter->second))
            {
                return std::get<Schema<UnqualifiedUnboundField, Ordered>>(schemaIter->second);
            }
        }
    }
    return std::nullopt;
}
}

std::optional<Schema<UnqualifiedUnboundField, Ordered>> getSourceSchema(ConfigMap configOptions)
{
    return getSchema(std::move(configOptions), Identifier::parse("SOURCE"));
}

std::optional<Schema<UnqualifiedUnboundField, Ordered>> getSinkSchema(ConfigMap configOptions)
{
    return getSchema(std::move(configOptions), Identifier::parse("SINK"));
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
    INVARIANT(false, "Unknow literal type, is the binder out of sync or was a nullptr passed?");
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
    /// Resolve the leading type-name token in isolation. When the rule also matches
    /// the optional `ARRAY '[' count ']'` suffix, calling `getText()` on the whole
    /// rule would concatenate everything into an unparseable string.
    /// The name is a DATA_TYPE token for built-ins (`UINT16`) and an IDENTIFIER token
    /// for plugin-registered named types (`Image`); exactly one of the two is present.
    auto* const typeNameToken = typeDefAST->DATA_TYPE() != nullptr ? typeDefAST->DATA_TYPE() : typeDefAST->IDENTIFIER();
    INVARIANT(typeNameToken != nullptr, "typeDefinition must carry either a DATA_TYPE or an IDENTIFIER token");
    std::string dataTypeText = typeNameToken->getText();

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

    /// `T ARRAY[N]` syntax → FIXEDSIZED. The element type comes from the leading
    /// DATA_TYPE token (must be primitive); the count is the bracketed integer.
    /// Constructed directly because `DataTypeRegistryArguments` only carries
    /// `nullable` and can't pass the element type / count through the registry.
    if (typeDefAST->ARRAY() != nullptr)
    {
        const auto elementType = DataTypeProvider::tryProvideDataType(dataTypeText, DataType::NULLABLE::NOT_NULLABLE);
        if (not elementType.has_value() || elementType->type == DataType::Type::VARSIZED
            || elementType->type == DataType::Type::FIXEDSIZED || elementType->type == DataType::Type::STRUCT
            || elementType->type == DataType::Type::UNDEFINED || elementType->type == DataType::Type::VARARRAY)
        {
            throw UnknownDataType(
                "{} is not a supported element type for `ARRAY[N]`; only primitive scalar types are allowed", dataTypeText);
        }
        const auto countText = typeDefAST->count->getText();
        uint32_t count = 0;
        try
        {
            count = static_cast<uint32_t>(std::stoul(countText));
        }
        catch (const std::exception&)
        {
            throw UnknownDataType("Could not parse FIXEDSIZED array count '{}' as a positive integer", countText);
        }
        if (count == 0)
        {
            throw UnknownDataType("FIXEDSIZED array count must be greater than zero");
        }
        return DataType{DataType::Type::FIXEDSIZED, isNullable, elementType.value(), count};
    }

    /// T VARARRAY -> Variablesized array with element type T -> map to VARARRAY type
    if (typeDefAST->VARARRAY() != nullptr)
    {
        const auto elementType = DataTypeProvider::tryProvideDataType(dataTypeText, DataType::NULLABLE::NOT_NULLABLE);
        if (not elementType.has_value() || elementType->type == DataType::Type::VARSIZED
            || elementType->type == DataType::Type::FIXEDSIZED || elementType->type == DataType::Type::STRUCT
            || elementType->type == DataType::Type::UNDEFINED || elementType->type == DataType::Type::VARARRAY)
        {
            throw UnknownDataType(
                "{} is not a supported element type for `VARARRAY`; only primitive scalar types are allowed", dataTypeText);
        }
        return DataType{DataType::Type::VARARRAY, isNullable, elementType.value()};
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
            [](const bool boolean) -> std::string { return boolean ? "true" : "false"; }},
        literal);
}
}
