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


#include <cctype>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>

#include <tuple>
#include <unordered_map>
#include <variant>

#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <fmt/format.h>
#include <sys/stat.h>
#include <Common/DataTypes/DataTypeProvider.hpp>

#include <ANTLRInputStream.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sources/SourceCatalog.hpp>
#include <ErrorHandling.hpp>
#include <SerializableOperator.pb.h>
#include <function.hpp>

namespace NES
{


class StatementBinder::Impl
{
    std::shared_ptr<const Catalogs::Source::SourceCatalog> sourceCatalog;
    std::function<std::shared_ptr<QueryPlan>(AntlrSQLParser::QueryContext*)> queryBinder;

public:
    using Literal = std::variant<std::string, int64_t, uint64_t, double, bool>;

    Impl(
        const std::shared_ptr<const Catalogs::Source::SourceCatalog>& sourceCatalog,
        const std::function<std::shared_ptr<QueryPlan>(AntlrSQLParser::QueryContext*)>& queryBinder)
        : sourceCatalog(sourceCatalog), queryBinder(queryBinder)
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
        throw InvalidIdentifier(strictIdentifier->getText());
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
                        throw InvalidLiteral("invalid escape sequence \'\\{}\' in literal \"{}\"", character, stringLiteral->getText());
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
        throw InvalidLiteral(literalAST->getText());
    }

    std::string literalToString(const Literal& literal) const
    {
        if (const auto* const stringLiteral = std::get_if<std::string>(&literal))
        {
            return *stringLiteral;
        }
        if (const auto* const intLiteral = std::get_if<int64_t>(&literal))
        {
            return std::to_string(*intLiteral);
        }
        if (const auto* const uintLiteral = std::get_if<uint64_t>(&literal))
        {
            return std::to_string(*uintLiteral);
        }
        if (const auto* const doubleLiteral = std::get_if<double>(&literal))
        {
            return std::to_string(*doubleLiteral);
        }
        if (const auto* const booleanLiteral = std::get_if<bool>(&literal))
        {
            return *booleanLiteral ? "true" : "false";
        }
        throw InvalidLiteral("Invalid literal");
    }


    /// TODO #897 replace with normal comparison binding
    std::pair<std::string, Literal> bindShowFilter(const AntlrSQLParser::ShowFilterContext* showFilterAST) const
    {
        return {bindIdentifier(showFilterAST->attr), bindLiteral(showFilterAST->value)};
    }

    ShowStatementFormat bindFormat(AntlrSQLParser::ShowFormatContext* formatAST) const
    {
        if (formatAST->TEXT() != nullptr)
        {
            return ShowStatementFormat::TEXT;
        }
        if (formatAST->JSON() != nullptr)
        {
            return ShowStatementFormat::JSON;
        }
        throw InvalidQuerySyntax("Invalid format");
    }

    CreateLogicalSourceStatement
    bindCreateLogicalSourceStatement(AntlrSQLParser::CreateLogicalSourceDefinitionContext* logicalSourceDefAST) const
    {
        const auto sourceName = logicalSourceDefAST->sourceName->getText();
        Schema schema{};

        for (auto* const column : logicalSourceDefAST->schemaDefinition()->columnDefinition())
        {
            if (auto dataType = DataTypeProvider::tryProvideDataType(column->typeDefinition()->getText()))
            {
                schema.addField(bindIdentifier(column->identifier()->strictIdentifier()), *dataType);
            }
            else
            {
                throw UnknownDataType(column->typeDefinition()->getText());
            }
        }
        return CreateLogicalSourceStatement{sourceName, schema};
    }

    CreatePhysicalSourceStatement
    bindCreatePhysicalSourceStatement(AntlrSQLParser::CreatePhysicalSourceDefinitionContext* physicalSourceDefAST) const
    {
        std::string type;
        WorkerId workerId = INITIAL<WorkerId>;
        std::unordered_map<std::string, std::string> sourceOptions{};
        std::unordered_map<std::string, std::string> parserOptions{};

        const auto logicalSourceOpt = sourceCatalog->getLogicalSource(physicalSourceDefAST->logicalSource->getText());
        if (not logicalSourceOpt.has_value())
        {
            throw UnregisteredSource("{}", physicalSourceDefAST->logicalSource->getText());
        }
        type = physicalSourceDefAST->type->getText();

        for (auto* options = physicalSourceDefAST->options; const auto& option : options->namedConfigExpression())
        {
            /// TODO use identifer list logic from #764
            if (option->name->strictIdentifier().size() != 2)
            {
                throw InvalidConfigParameter("{}", option->name->getText());
            }
            const auto rootIdentifier = bindIdentifier(option->name->strictIdentifier().at(0));
            auto optionName = bindIdentifier(option->name->strictIdentifier().at(1));
            auto optionValue = bindLiteral(option->constant());
            if (rootIdentifier == "SOURCE")
            {
                if (optionName == "BUFFERS_IN_LOCAL_POOL")
                {
                    optionName = "buffersInLocalPool";
                }
                if (optionName == "FILE_PATH")
                {
                    optionName = "filePath";
                }
                if (optionName == "LOCATION")
                {
                    if (auto* stringValue = std::get_if<std::string>(&optionValue))
                    {
                        if (*stringValue != "LOCAL")
                        {
                            throw InvalidConfigParameter(
                                R"(SOURCE.LOCATION must be either an integer or 'LOCAL' but was "{}")", std::get<std::string>(optionValue));
                        }
                    }
                    else if (auto* const intValue = std::get_if<int64_t>(&optionValue))
                    {
                        if (*intValue < 0)
                        {
                            throw InvalidConfigParameter("SOURCE.LOCATION cannot be a negative integer, but was {}", *intValue);
                        }
                        workerId = WorkerId{static_cast<uint64_t>(*intValue)};
                    }
                }
                else
                {
                    if (not sourceOptions.try_emplace(optionName, literalToString(bindLiteral(option->constant()))).second)
                    {
                        throw InvalidConfigParameter("Duplicate option for source: {}", option->name->getText());
                    }
                }
            }
            else if (rootIdentifier == "PARSER")
            {
                //replace snake case caps with camel case option names
                if (optionName == "TUPLE_DELIMITER")
                {
                    optionName = "tupleDelimiter";
                }
                else if (optionName == "FIELD_DELIMITER")
                {
                    optionName = "fieldDelimiter";
                }
                else if (optionName == "TYPE")
                {
                    optionName = "type";
                }
                if (not std::holds_alternative<std::string>(optionValue))
                {
                    throw InvalidConfigParameter(
                        "Parser option {} must be a string, but was {} ", option->name->getText(), option->constant()->getText());
                }
                if (const auto [iter, inserted] = parserOptions.try_emplace(optionName, std::get<std::string>(optionValue)); not inserted)
                {
                    throw InvalidConfigParameter("Duplicate option for parser: {}", option->name->getText());
                }
            }
        }

        auto parserConfig = Sources::ParserConfig::create(parserOptions);
        //Validate input formatter config and type. We don't attach it directly to the SourceDescriptor to avoid the dependencies
        InputFormatters::InputFormatterProvider::provideInputFormatter(
            parserConfig.parserType, *logicalSourceOpt->getSchema(), parserConfig.tupleDelimiter, parserConfig.fieldDelimiter);
        if (not sourceOptions.try_emplace("type", type).second)
        {
            throw InvalidConfigParameter("Type option for source cannot be specified via SET");
        }
        return CreatePhysicalSourceStatement{logicalSourceOpt.value(), type, workerId, sourceOptions, parserConfig};
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
        throw InvalidStatement("Unrecognized CREATE statement");
    }

    ShowLogicalSourcesStatement bindShowLogicalSourcesStatement(
        const AntlrSQLParser::ShowFilterContext* showFilter, AntlrSQLParser::ShowFormatContext* showFormat) const
    {
        const ShowStatementFormat format = showFormat != nullptr ? bindFormat(showFormat) : ShowStatementFormat::TEXT;
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
        const ShowStatementFormat format = showFormat != nullptr ? bindFormat(showFormat) : ShowStatementFormat::TEXT;
        if (physicalSourcesSubject->logicalSourceName != nullptr)
        {
            const auto logicalSourceName = bindIdentifier(physicalSourcesSubject->logicalSourceName);
            logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
            if (not logicalSource.has_value())
            {
                throw UnregisteredSource("There is no logical source with name {}", logicalSourceName);
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

    ShowQueriesStatement
    bindShowQueriesStatement(const AntlrSQLParser::ShowFilterContext* showFilter, AntlrSQLParser::ShowFormatContext* showFormat) const
    {
        const ShowStatementFormat format = showFormat != nullptr ? bindFormat(showFormat) : ShowStatementFormat::TEXT;
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
            return ShowQueriesStatement{.id = QueryId{std::get<uint64_t>(value)}, .format = format};
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
        throw InvalidStatement("Unrecognized SHOW statement");
    }

    Statement bindDropStatement(AntlrSQLParser::DropStatementContext* dropAst) const
    {
        if (AntlrSQLParser::DropSourceContext* dropSourceAst = dropAst->dropSubject()->dropSource(); dropSourceAst != nullptr)
        {
            if (const auto* const logicalSourceSubject = dropSourceAst->dropLogicalSourceSubject();
                logicalSourceSubject != nullptr && sourceCatalog->containsLogicalSource(logicalSourceSubject->name->getText()))
            {
                if (const auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceSubject->name->getText());
                    logicalSource.has_value())
                {
                    return DropLogicalSourceStatement{*logicalSource};
                }
                throw UnregisteredSource(logicalSourceSubject->name->getText());
            }
            if (const auto* const physicalSourceSubject = dropSourceAst->dropPhysicalSourceSubject(); physicalSourceSubject != nullptr)
            {
                if (const auto physicalSource = sourceCatalog->getPhysicalSource(bindUnsignedIntegerLiteral(physicalSourceSubject->id));
                    physicalSource.has_value())
                {
                    return DropPhysicalSourceStatement{*physicalSource};
                }
                throw UnregisteredSource("There is no physical source with id {}", physicalSourceSubject->id->getText());
            }
        }
        else if (const auto* const dropQueryAst = dropAst->dropSubject()->dropQuery(); dropQueryAst != nullptr)
        {
            const auto id = QueryId{std::stoul(dropQueryAst->id->getText())};
            return DropQueryStatement{id};
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
    }
};


StatementBinder::StatementBinder(
    const std::shared_ptr<const Catalogs::Source::SourceCatalog>& sourceCatalog,
    const std::function<std::shared_ptr<QueryPlan>(AntlrSQLParser::QueryContext*)>& queryPlanBinder) noexcept
    : impl(std::make_unique<Impl>(sourceCatalog, queryPlanBinder))
{
}
StatementBinder::~StatementBinder() = default;
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
               "CreatePhysicalSourceStatement: attachedTo: {} sourceType: {} workerId: {} sourceConfig: {} parserConfig: {}",
               obj.attachedTo,
               obj.sourceType,
               obj.workerId,
               obj.sourceConfig,
               obj.parserConfig);
}
}
