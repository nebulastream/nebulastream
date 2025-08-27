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

#include <cctype>
#include <climits>
#include <cstdint>
#include <exception>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <Util/Strings.hpp>

#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataTypeProvider.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceValidationProvider.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>

#include <ANTLRInputStream.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <BailErrorStrategy.h>
#include <CommonTokenStream.h>
#include <Exceptions.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// NOLINTBEGIN(readability-convert-member-functions-to-static)
class StatementBinder::Impl
{
    std::shared_ptr<const SourceCatalog> sourceCatalog;
    std::shared_ptr<const SinkCatalog> sinkCatalog;
    std::function<LogicalPlan(AntlrSQLParser::QueryContext*)> queryBinder;

public:
    using Literal = std::variant<std::string, int64_t, uint64_t, double, bool>;

    Impl(
        const std::shared_ptr<const SourceCatalog>& sourceCatalog,
        const std::shared_ptr<const SinkCatalog>& sinkCatalog,
        const std::function<LogicalPlan(AntlrSQLParser::QueryContext*)>& queryBinder)
        : sourceCatalog(sourceCatalog), sinkCatalog(sinkCatalog), queryBinder(queryBinder)
    {
    }

    ~Impl() = default;

    std::string bindIdentifier(AntlrSQLParser::StrictIdentifierContext* strictIdentifier) const
    {
        if (auto* const unquotedIdentifier = dynamic_cast<AntlrSQLParser::UnquotedIdentifierContext*>(strictIdentifier))
        {
            std::string text = unquotedIdentifier->getText();
            return text | std::ranges::views::transform([](const char character) { return std::toupper(character); })
                | std::ranges::to<std::string>();
        }
        if (auto* const quotedIdentifier = dynamic_cast<AntlrSQLParser::QuotedIdentifierAlternativeContext*>(strictIdentifier))
        {
            const auto withQuotationMarks = quotedIdentifier->quotedIdentifier()->BACKQUOTED_IDENTIFIER()->getText();
            return withQuotationMarks.substr(1, withQuotationMarks.size() - 2);
        }
        INVARIANT(
            false,
            "Unknown identifier type, was neither valid quoted or unquoted, is the grammar out of sync with the binder or was a nullptr "
            "passed?");
        std::unreachable();
    }

    std::string bindStringLiteral(AntlrSQLParser::StringLiteralContext* stringLiteral) const
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

    int64_t bindIntegerLiteral(AntlrSQLParser::IntegerLiteralContext* integerLiteral) const { return std::stoi(integerLiteral->getText()); }

    int64_t bindIntegerLiteral(AntlrSQLParser::SignedIntegerLiteralContext* signedIntegerLiteral) const
    {
        return std::stoi(signedIntegerLiteral->getText());
    }

    uint64_t bindUnsignedIntegerLiteral(AntlrSQLParser::UnsignedIntegerLiteralContext* unsignedIntegerLiteral) const
    {
        return std::stoul(unsignedIntegerLiteral->getText());
    }

    double bindDoubleLiteral(AntlrSQLParser::FloatLiteralContext* doubleLiteral) const { return std::stod(doubleLiteral->getText()); }

    bool bindBooleanLiteral(AntlrSQLParser::BooleanLiteralContext* booleanLiteral) const
    {
        return booleanLiteral->getText() == "true" || booleanLiteral->getText() == "TRUE";
    }

    Literal bindLiteral(AntlrSQLParser::ConstantContext* literalAST) const
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

    [[nodiscard]] std::string literalToString(const Literal& literal) const
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

    /// TODO #897 replace with normal comparison binding
    std::pair<std::string, Literal> bindShowFilter(const AntlrSQLParser::ShowFilterContext* showFilterAST) const
    {
        return {bindIdentifier(showFilterAST->attr), bindLiteral(showFilterAST->value)};
    }

    StatementOutputFormat bindFormat(AntlrSQLParser::ShowFormatContext* formatAST) const
    {
        if (formatAST->TEXT() != nullptr)
        {
            return StatementOutputFormat::TEXT;
        }
        if (formatAST->JSON() != nullptr)
        {
            return StatementOutputFormat::JSON;
        }
        INVARIANT(false, "Invalid format type, is the binder out of sync or was a nullptr passed?");
        std::unreachable();
    }

    DataType bindDataType(AntlrSQLParser::TypeDefinitionContext* typeDefAST) const
    {
        std::string dataTypeText = typeDefAST->getText();

        bool translated = false;
        bool isUnsigned = false;
        if (dataTypeText.starts_with("UNSIGNED "))
        {
            isUnsigned = true;
            translated = true;
            dataTypeText = dataTypeText.substr(9);
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

    Schema bindSchema(AntlrSQLParser::SchemaDefinitionContext* schemaDefAST) const
    {
        Schema schema{};

        for (auto* const column : schemaDefAST->columnDefinition())
        {
            auto dataType = bindDataType(column->typeDefinition());
            /// TODO #764 Remove qualification of column names in schema declarations, it's only needed as a hack now to make it work with the per-operator-lexical-scopes.
            std::stringstream qualifiedAttributeName;
            for (const auto& unboundIdentifier : column->identifierChain()->strictIdentifier())
            {
                qualifiedAttributeName << bindIdentifier(unboundIdentifier) << "$";
            }
            const auto fullName = qualifiedAttributeName.str().substr(0, qualifiedAttributeName.str().size() - 1);
            schema.addField(fullName, dataType);
        }
        return schema;
    }

    CreateLogicalSourceStatement
    bindCreateLogicalSourceStatement(AntlrSQLParser::CreateLogicalSourceDefinitionContext* logicalSourceDefAST) const
    {
        const auto sourceName = bindIdentifier(logicalSourceDefAST->sourceName->strictIdentifier());
        const auto schema = bindSchema(logicalSourceDefAST->schemaDefinition());
        return CreateLogicalSourceStatement{.name = sourceName, .schema = schema};
    }

    /// TODO #764 use identifier lists instead of map of maps
    [[nodiscard]] std::unordered_map<std::string, std::unordered_map<std::string, Literal>>
    bindConfigOptions(const std::vector<AntlrSQLParser::NamedConfigExpressionContext*>& configOptions) const
    {
        std::unordered_map<std::string, std::unordered_map<std::string, Literal>> boundConfigOptions{};
        for (auto* const configOption : configOptions)
        {
            if (configOption->name->strictIdentifier().size() != 2)
            {
                throw InvalidConfigParameter("Config key needs to be qualified exactly once, but was {}", configOption->name->getText());
            }
            const auto rootIdentifier = bindIdentifier(configOption->name->strictIdentifier().at(0));
            auto optionName = bindIdentifier(configOption->name->strictIdentifier().at(1));
            boundConfigOptions.try_emplace(rootIdentifier, std::unordered_map<std::string, Literal>{});
            if (not boundConfigOptions.at(rootIdentifier).try_emplace(optionName, bindLiteral(configOption->constant())).second)
            {
                throw InvalidConfigParameter("Duplicate option for source: {}", configOption->name->getText());
            }
        }
        return boundConfigOptions;
    }

    CreatePhysicalSourceStatement
    bindCreatePhysicalSourceStatement(AntlrSQLParser::CreatePhysicalSourceDefinitionContext* physicalSourceDefAST) const
    {
        std::unordered_map<std::string, std::string> sourceOptions{};
        const std::unordered_map<std::string, std::string> parserOptions{};

        const auto logicalSourceName = bindIdentifier(physicalSourceDefAST->logicalSource->strictIdentifier());
        const auto logicalSourceOpt = sourceCatalog->getLogicalSource(logicalSourceName);
        if (not logicalSourceOpt.has_value())
        {
            throw UnknownSourceName("{}", logicalSourceName);
        }
        /// TODO #764 use normal identifiers for types
        const std::string type = physicalSourceDefAST->type->getText();

        auto configOptions = bindConfigOptions(physicalSourceDefAST->options->namedConfigExpression());
        const auto parserConfigIter = configOptions.find("PARSER");
        if (parserConfigIter == configOptions.end() || not parserConfigIter->second.contains("TYPE"))
        {
            throw InvalidConfigParameter("Parser type not specified");
        }

        const ParserConfig parserConfig = ParserConfig::create(
            parserConfigIter->second
            | std::views::transform([this](const auto& pair)
                                    { return std::make_pair(NES::Util::snakeToCamelCase(pair.first), literalToString(pair.second)); })
            | std::ranges::to<std::unordered_map<std::string, std::string>>());

        if (const auto sourceConfigIter = configOptions.find("SOURCE"); sourceConfigIter != configOptions.end())
        {
            sourceOptions = sourceConfigIter->second
                | std::views::transform([this](auto& pair)
                                        { return std::make_pair(NES::Util::snakeToCamelCase(pair.first), literalToString(pair.second)); })
                | std::ranges::to<std::unordered_map<std::string, std::string>>();
        }

        /// Validate input formatter and source config and type. We don't attach it directly to the SourceDescriptor to avoid the dependencies
        if (not InputFormatters::InputFormatterProvider::contains(parserConfig.parserType))
        {
            throw InvalidConfigParameter("Invalid parser type {}", parserConfig.parserType);
        }
        if (not Sources::SourceValidationProvider::provide(type, sourceOptions).has_value())
        {
            /// We probably want to propagate the error from the descriptor validation up to here somehow, so that the user can receive a detailed error message.
            throw InvalidConfigParameter("Invalid source configuration for type {} with arguments {}", type, sourceOptions);
        }
        return CreatePhysicalSourceStatement{
            .attachedTo = logicalSourceOpt.value(), .sourceType = type, .workerId = "", .sourceConfig = sourceOptions, .parserConfig = parserConfig};
    }

    CreateSinkStatement bindCreateSinkStatement(AntlrSQLParser::CreateSinkDefinitionContext* sinkDefAST) const
    {
        const auto sinkName = bindIdentifier(sinkDefAST->sinkName->strictIdentifier());
        const auto sinkType = sinkDefAST->type->getText();
        const auto configOptions = bindConfigOptions(sinkDefAST->options->namedConfigExpression());
        std::unordered_map<std::string, std::string> sinkOptions{};
        if (const auto sinkConfigIter = configOptions.find("SINK"); sinkConfigIter != configOptions.end())
        {
            sinkOptions = sinkConfigIter->second
                | std::views::transform([this](auto& pair)
                                        { return std::make_pair(NES::Util::snakeToCamelCase(pair.first), literalToString(pair.second)); })
                | std::ranges::to<std::unordered_map<std::string, std::string>>();
        }
        if (not Sinks::SinkDescriptor::validateAndFormatConfig(sinkType, sinkOptions).has_value())
        {
            throw InvalidConfigParameter("Invalid sink configuration");
        }
        const auto schema = bindSchema(sinkDefAST->schemaDefinition());
        return CreateSinkStatement{.name = sinkName, .sinkType = sinkType, .schema = schema, .workerId = "", .sinkConfig = sinkOptions};
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
        throw InvalidStatement("Unrecognized CREATE statement");
    }

    ShowLogicalSourcesStatement bindShowLogicalSourcesStatement(
        const AntlrSQLParser::ShowFilterContext* showFilter, AntlrSQLParser::ShowFormatContext* showFormat) const
    {
        const std::optional<StatementOutputFormat> format
            = showFormat != nullptr ? std::make_optional(bindFormat(showFormat)) : std::nullopt;
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            if (attr != "NAME")
            {
                throw InvalidQuerySyntax("Filter for SHOW LOGICAL SOURCES must be on name attribute");
            }
            if (not std::holds_alternative<std::string>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW LOGICAL SOURCES must be a string");
            }
            return ShowLogicalSourcesStatement{.name = std::get<std::string>(value), .format = format};
        }
        return ShowLogicalSourcesStatement{.name = std::nullopt, .format = format};
    }

    ShowPhysicalSourcesStatement bindShowPhysicalSourcesStatement(
        const AntlrSQLParser::ShowFilterContext* showFilter,
        const AntlrSQLParser::ShowPhysicalSourcesSubjectContext* physicalSourcesSubject,
        AntlrSQLParser::ShowFormatContext* showFormat) const
    {
        std::optional<LogicalSource> logicalSource{};
        const std::optional<StatementOutputFormat> format
            = showFormat != nullptr ? std::make_optional(bindFormat(showFormat)) : std::nullopt;
        if (physicalSourcesSubject->logicalSourceName != nullptr)
        {
            const auto logicalSourceName = bindIdentifier(physicalSourcesSubject->logicalSourceName);
            logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
            if (not logicalSource.has_value())
            {
                throw UnknownSourceName("There is no logical source with name {}", logicalSourceName);
            }
        }
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            if (attr != "ID")
            {
                throw InvalidQuerySyntax("Filter for SHOW PHYSICAL SOURCES must be on id attribute");
            }
            if (not std::holds_alternative<uint64_t>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW PHYSICAL SOURCES must be an unsigned integer");
            }
            return ShowPhysicalSourcesStatement{.logicalSource = logicalSource, .id = std::get<uint64_t>(value), .format = format};
        }
        return ShowPhysicalSourcesStatement{.logicalSource = logicalSource, .id = std::nullopt, .format = format};
    }

    ShowSinksStatement
    bindShowSinksStatement(const AntlrSQLParser::ShowFilterContext* showFilter, AntlrSQLParser::ShowFormatContext* showFormat) const
    {
        const std::optional<StatementOutputFormat> format
            = showFormat != nullptr ? std::make_optional(bindFormat(showFormat)) : std::nullopt;
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            if (attr != "NAME")
            {
                throw InvalidQuerySyntax("Filter for SHOW SINKS must be on name attribute");
            }
            if (not std::holds_alternative<std::string>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW SINKS must be a string");
            }
            return ShowSinksStatement{.name = std::get<std::string>(value), .format = format};
        }
        return ShowSinksStatement{.name = std::nullopt, .format = format};
    }

    ShowQueriesStatement
    bindShowQueriesStatement(const AntlrSQLParser::ShowFilterContext* showFilter, AntlrSQLParser::ShowFormatContext* showFormat) const
    {
        const std::optional<StatementOutputFormat> format
            = showFormat != nullptr ? std::make_optional(bindFormat(showFormat)) : std::nullopt;
        if (showFilter != nullptr)
        {
            const auto [attr, value] = bindShowFilter(showFilter);
            if (attr != "ID")
            {
                throw InvalidQuerySyntax("Filter for SHOW QUERIES must be on id attribute");
            }
            if (not std::holds_alternative<uint64_t>(value))
            {
                throw InvalidQuerySyntax("Filter value for SHOW QUERIES must be an unsigned integer");
            }
            return ShowQueriesStatement{.id = DistributedQueryId{std::get<uint64_t>(value)}, .format = format};
        }
        return ShowQueriesStatement{.id = std::nullopt, .format = format};
    }

    Statement bindShowStatement(AntlrSQLParser::ShowStatementContext* showAST) const
    {
        auto* showFilter = showAST->showFilter();

        if (const auto* logicalSourcesSubject = dynamic_cast<AntlrSQLParser::ShowLogicalSourcesSubjectContext*>(showAST->showSubject());
            logicalSourcesSubject != nullptr)
        {
            return bindShowLogicalSourcesStatement(showFilter, showAST->showFormat());
        }
        if (auto* physicalSourcesSubject = dynamic_cast<AntlrSQLParser::ShowPhysicalSourcesSubjectContext*>(showAST->showSubject());
            physicalSourcesSubject != nullptr)
        {
            return bindShowPhysicalSourcesStatement(showFilter, physicalSourcesSubject, showAST->showFormat());
        }
        if (const auto* queriesSubject = dynamic_cast<AntlrSQLParser::ShowQueriesSubjectContext*>(showAST->showSubject());
            queriesSubject != nullptr)
        {
            return bindShowQueriesStatement(showFilter, showAST->showFormat());
        }
        if (const auto* sinksSubject = dynamic_cast<AntlrSQLParser::ShowSinksSubjectContext*>(showAST->showSubject());
            sinksSubject != nullptr)
        {
            return bindShowSinksStatement(showFilter, showAST->showFormat());
        }
        throw InvalidStatement("Unrecognized SHOW statement");
    }

    Statement bindDropStatement(AntlrSQLParser::DropStatementContext* dropAst) const
    {
        if (AntlrSQLParser::DropSourceContext* dropSourceAst = dropAst->dropSubject()->dropSource(); dropSourceAst != nullptr)
        {
            if (const auto* const logicalSourceSubject = dropSourceAst->dropLogicalSourceSubject(); logicalSourceSubject != nullptr)
            {
                if (const auto logicalSourceName = bindIdentifier(logicalSourceSubject->name);
                    sourceCatalog->containsLogicalSource(logicalSourceName))
                {
                    if (const auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName); logicalSource.has_value())
                    {
                        return DropLogicalSourceStatement{*logicalSource};
                    }
                    throw UnknownSourceName(logicalSourceSubject->name->getText());
                }
            }
            if (const auto* const physicalSourceSubject = dropSourceAst->dropPhysicalSourceSubject(); physicalSourceSubject != nullptr)
            {
                if (const auto physicalSource
                    = sourceCatalog->getPhysicalSource(PhysicalSourceId{bindUnsignedIntegerLiteral(physicalSourceSubject->id)});
                    physicalSource.has_value())
                {
                    return DropPhysicalSourceStatement{*physicalSource};
                }
                throw UnknownSourceName("There is no physical source with id {}", physicalSourceSubject->id->getText());
            }
        }
        else if (const auto* const dropQueryAst = dropAst->dropSubject()->dropQuery(); dropQueryAst != nullptr)
        {
            const auto id = DistributedQueryId{std::stoul(dropQueryAst->id->getText())};
            return DropQueryStatement{id};
        }
        else if (const auto* const dropSinkAst = dropAst->dropSubject()->dropSink(); dropSinkAst != nullptr)
        {
            const auto sinkName = bindIdentifier(dropSinkAst->name);
            if (const auto sink = sinkCatalog->getSinkDescriptor(sinkName); sink.has_value())
            {
                return DropSinkStatement{*sink};
            }
            throw UnknownSinkName(sinkName);
        }
        throw InvalidStatement("Unrecognized DROP statement");
    }

    std::expected<Statement, Exception> bind(AntlrSQLParser::StatementContext* statementAST) const
    {
        if (statementAST->query() != nullptr)
        {
            return queryBinder(statementAST->query());
        }
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
            if (auto* const queryAst = statementAST->query(); queryAst != nullptr)
            {
                return queryBinder(queryAst);
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
    const std::shared_ptr<const SourceCatalog>& sourceCatalog,
    const std::shared_ptr<const SinkCatalog>& sinkCatalog,
    const std::function<LogicalPlan(AntlrSQLParser::QueryContext*)>& queryPlanBinder) noexcept
    : impl(std::make_unique<Impl>(sourceCatalog, sinkCatalog, queryPlanBinder))
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

std::expected<Statement, Exception> StatementBinder::bind(AntlrSQLParser::StatementContext* statementAST) const noexcept
{
    return impl->bind(statementAST);
}

std::expected<std::vector<std::expected<Statement, Exception>>, Exception>
StatementBinder::parseAndBind(const std::string_view statementString) const noexcept
{
    try
    {
        antlr4::ANTLRInputStream input(statementString.data(), statementString.length());
        AntlrSQLLexer lexer(&input);
        antlr4::CommonTokenStream tokens(&lexer);
        AntlrSQLParser parser(&tokens);
        /// Enable that antlr throws exeptions on parsing errors
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

std::expected<Statement, Exception> StatementBinder::parseAndBindSingle(std::string_view statementStrings) const noexcept
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
               "CreatePhysicalSourceStatement: attachedTo: {} sourceType: {} sourceConfig: {} parserConfig: {}",
               obj.attachedTo,
               obj.sourceType,
               obj.sourceConfig,
               obj.parserConfig);
}

/// NOLINTEND(readability-convert-member-functions-to-static)
}
