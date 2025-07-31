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
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>

#include <AntlrSQLParser.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundSchema.hpp>
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
        throw idOpt.error();
    }
    return idOpt.value();
}

IdentifierList bindIdentifierList(AntlrSQLParser::IdentifierChainContext* identifierList)
{
    return identifierList->strictIdentifier()
        | std::views::transform([](AntlrSQLParser::StrictIdentifierContext* identifier) { return bindIdentifier(identifier); })
        | std::ranges::to<IdentifierList>();
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
        boundConfigOptions.try_emplace(rootIdentifier, std::unordered_map<Identifier, std::variant<Literal, SchemaBase<UnboundFieldBase<1>, true>>>{});

        std::variant<Literal, SchemaBase<UnboundFieldBase<1>, true>> value{};

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

std::unordered_map<Identifier, std::string> getParserConfig(const ConfigMap& configOptions)
{
    auto parserConfig = std::unordered_map<Identifier, std::string>{};

    if (const auto parserConfigIter = configOptions.find(Identifier::parse("PARSER")); parserConfigIter != configOptions.end())
    {
        parserConfig = parserConfigIter->second
            | std::views::filter([](auto& pair) { return std::holds_alternative<Literal>(pair.second); })
            | std::views::transform(
                           [](auto& pair)
                           { return std::make_pair(pair.first, literalToString(std::get<Literal>(pair.second))); })
            | std::ranges::to<std::unordered_map<Identifier, std::string>>();
    }
    return parserConfig;
}

std::unordered_map<Identifier, std::string> getSourceConfig(const ConfigMap& configOptions)
{
    std::unordered_map<Identifier, std::string> sourceOptions{};
    if (const auto sourceConfigIter = configOptions.find(Identifier::parse("SOURCE")); sourceConfigIter != configOptions.end())
    {
        sourceOptions = sourceConfigIter->second
            | std::views::filter([](auto& pair) { return std::holds_alternative<Literal>(pair.second); })
            | std::views::transform(
                            [](auto& pair)
                            { return std::make_pair(pair.first, literalToString(std::get<Literal>(pair.second))); })
            | std::ranges::to<std::unordered_map<Identifier, std::string>>();
    }

    return sourceOptions;
}

std::unordered_map<Identifier, std::string> getSinkConfig(const ConfigMap& configOptions)
{
    std::unordered_map<Identifier, std::string> sinkOptions{};
    if (const auto sourceConfigIter = configOptions.find(Identifier::parse("SINK")); sourceConfigIter != configOptions.end())
    {

        sinkOptions = sourceConfigIter->second | std::views::filter([](auto& pair) { return std::holds_alternative<Literal>(pair.second); })
            | std::views::transform(
                          [](auto& pair)
                          { return std::make_pair(pair.first, literalToString(std::get<Literal>(pair.second))); })
            | std::ranges::to<std::unordered_map<Identifier, std::string>>();
    }

    return sinkOptions;
}

namespace
{
std::optional<SchemaBase<UnboundFieldBase<1>, true>> getSchema(ConfigMap configOptions, const Identifier& configName)
{
    if (const auto sourceConfigIter = configOptions.find(configName); sourceConfigIter != configOptions.end())
    {
        if (const auto schemaIter = sourceConfigIter->second.find(Identifier::parse("SCHEMA"));
            schemaIter != sourceConfigIter->second.end())
        {
            if (std::holds_alternative<SchemaBase<UnboundFieldBase<1>, true>>(schemaIter->second))
            {
                return std::get<SchemaBase<UnboundFieldBase<1>, true>>(schemaIter->second);
            }
        }
    }
    return std::nullopt;
}
}

std::optional<SchemaBase<UnboundFieldBase<1>, true>> getSourceSchema(ConfigMap configOptions)
{
    return getSchema(std::move(configOptions), Identifier::parse("SOURCE"));
}

std::optional<SchemaBase<UnboundFieldBase<1>, true>> getSinkSchema(ConfigMap configOptions)
{
    return getSchema(std::move(configOptions), Identifier::parse("SINK"));
}

std::string bindStringLiteral(AntlrSQLParser::StringLiteralContext* stringLiteral)
{
    PRECONDITION(stringLiteral->getText().size() > 1, "String literal must have at least two characters for quotation marks");
    bool inEscapeSequence = false;
    std::stringstream ss;
    for (uint32_t i = 1; i < stringLiteral->getText().size() - 1; i++)
    {
        const char character = stringLiteral->getText()[i];
        if (inEscapeSequence)
        {
            switch (character)
            {
                case 'n':
                    ss << '\n';
                    break;
                case 'r':
                    ss << '\r';
                    break;
                case 't':
                    ss << '\t';
                    break;
                case '\\':
                    ss << '\\';
                default:
                    throw InvalidLiteral(R"(invalid escape sequence '\{}' in literal "{}")", character, stringLiteral->getText());
            }
        }
        else
        {
            if (character == '\\')
            {
                inEscapeSequence = true;
            }
            else
            {
                ss << character;
            }
        }
    }
    return ss.str();
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

SchemaBase<UnboundFieldBase<1>, true> bindSchema(AntlrSQLParser::SchemaDefinitionContext* schemaDefAST)
{
    std::vector<UnboundFieldBase<1>> fields{};

    for (auto* const column : schemaDefAST->columnDefinition())
    {
        const auto dataType = bindDataType(column->typeDefinition());
        auto columnName = bindIdentifier(column->strictIdentifier());
        fields.push_back(UnboundFieldBase<1>{columnName, dataType});
    }
    return fields | std::ranges::to<SchemaBase<UnboundFieldBase<1>, true>>();
}

DataType bindDataType(AntlrSQLParser::TypeDefinitionContext* typeDefAST)
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
    const auto dataType = DataTypeProvider::tryProvideDataType(dataTypeText);
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
