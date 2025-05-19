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
#include <memory>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>

#include <tuple>
#include <unordered_map>
#include <variant>
#include <sys/stat.h>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include "Functions/NodeFunction.hpp"
#include "Functions/NodeFunctionConstantValue.hpp"
#include "Sources/SourceDescriptor.hpp"

#include <ANTLRInputStream.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sources/SourceCatalog.hpp>
#include <ErrorHandling.hpp>
#include <SerializableOperator.pb.h>
#include <function.hpp>

namespace NES::Binder
{
StatementBinder::StatementBinder(
    const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog,
    const std::function<std::shared_ptr<QueryPlan>(AntlrSQLParser::QueryContext*)>& queryPlanBinder) noexcept
    : sourceCatalog(sourceCatalog), queryBinder(queryPlanBinder)
{
}

static_assert(std::ranges::range<std::string>);

std::string bindIdentifier(AntlrSQLParser::StrictIdentifierContext* strictIdentifier)
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

std::string bindStringLiteral(AntlrSQLParser::StringLiteralContext* stringLiteral)
{
    PRECONDITION(stringLiteral->getText().size() > 1, "String literal must have at least two characters");
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

int64_t bindIntegerLiteral(AntlrSQLParser::IntegerLiteralContext* integerLiteral)
{
    return std::stoi(integerLiteral->getText());
}

int64_t bindIntegerLiteral(AntlrSQLParser::SignedIntegerLiteralContext* signedIntegerLiteral)
{
    return std::stoi(signedIntegerLiteral->getText());
}

uint64_t bindUnsignedIntegerLiteral(AntlrSQLParser::UnsignedIntegerLiteralContext* unsignedIntegerLiteral)
{
    return std::stoul(unsignedIntegerLiteral->getText());
}

double bindDoubleLiteral(AntlrSQLParser::FloatLiteralContext* doubleLiteral)
{
    return std::stod(doubleLiteral->getText());
}
bool bindBooleanLiteral(AntlrSQLParser::BooleanLiteralContext* booleanLiteral)
{
    return booleanLiteral->getText() == "true" || booleanLiteral->getText() == "TRUE";
}

std::variant<std::string, int64_t, double, bool> bindLiteral(AntlrSQLParser::ConstantContext* literalAST)
{
    if (auto* const stringAST = dynamic_cast<AntlrSQLParser::StringLiteralContext*>(literalAST))
    {
        return bindStringLiteral(stringAST);
    }
    if (auto* const numericLiteral = dynamic_cast<AntlrSQLParser::NumericLiteralContext*>(literalAST))
    {
        if (auto* const intLocation = dynamic_cast<AntlrSQLParser::IntegerLiteralContext*>(numericLiteral->number()))
        {
            return bindIntegerLiteral(intLocation);
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

std::string literalToString(const std::variant<std::string, int64_t, double, bool>& literal)
{
    if (const auto* const stringLiteral = std::get_if<std::string>(&literal))
    {
        return *stringLiteral;
    }
    if (const auto* const intLiteral = std::get_if<int64_t>(&literal))
    {
        return std::to_string(*intLiteral);
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

Statement StatementBinder::bind(AntlrSQLParser::StatementContext* statementAST)
{
    if (statementAST->query() != nullptr)
    {
        return queryBinder(statementAST->query());
    }
    if (auto* const createAST = statementAST->createStatement(); createAST != nullptr)
    {
        if (auto* const logicalSourceDefAST = createAST->createDefinition()->createLogicalSourceDefinition();
            logicalSourceDefAST != nullptr)
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
            const auto addedSourceOpt = sourceCatalog->addLogicalSource(sourceName, schema);

            if (not addedSourceOpt.has_value())
            {
                return CreateLogicalSourceStatement{std::unexpected(SourceAlreadyExists(sourceName))};
            }

            return CreateLogicalSourceStatement{addedSourceOpt.value()};
        }
        if (auto* const physicalSourceDefAST = createAST->createDefinition()->createPhysicalSourceDefinition();
            physicalSourceDefAST != nullptr)
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
                                    R"(SOURCE.LOCATION must be either an integer or 'LOCAL' but was "{}")",
                                    std::get<std::string>(optionValue));
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
                    if (const auto [iter, inserted] = parserOptions.try_emplace(optionName, std::get<std::string>(optionValue));
                        not inserted)
                    {
                        throw InvalidConfigParameter("Duplicate option for parser: {}", option->name->getText());
                    }
                }
            }
            auto parserConfig = Sources::ParserConfig::create(parserOptions);
            if (not sourceOptions.try_emplace("type", type).second)
            {
                throw InvalidConfigParameter("Type option for source cannot be specified via SET");
            }
            auto source = sourceCatalog->addPhysicalSource(logicalSourceOpt.value(), type, workerId, sourceOptions, parserConfig);
            if (not source.has_value())
            {
                throw InvalidConfigParameter("Invalid source configuration: {}", statementAST->getText());
            }

            return CreatePhysicalSourceStatement{source.value()};
        }
    }
    if (auto* dropAst = statementAST->dropStatement(); dropAst != nullptr)
    {
        if (AntlrSQLParser::DropSourceContext* dropSourceAst = dropAst->dropSubject()->dropSource(); dropSourceAst != nullptr)
        {
            if (const auto* const logicalSourceSubject = dropSourceAst->dropLogicalSourceSubject();
                logicalSourceSubject != nullptr && sourceCatalog->containsLogicalSource(logicalSourceSubject->name->getText()))
            {
                if (const auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceSubject->name->getText());
                    logicalSource.has_value())
                {
                    if (const auto success = sourceCatalog->removeLogicalSource(logicalSource.value()))
                    {
                        return DropLogicalSourceStatement{*logicalSource};
                    }
                    return DropLogicalSourceStatement{std::unexpected(UnregisteredSource(logicalSourceSubject->name->getText()))};
                }
                throw UnregisteredSource(logicalSourceSubject->name->getText());
            }
            if (const auto* const physicalSourceSubject = dropSourceAst->dropPhysicalSourceSubject(); physicalSourceSubject != nullptr)
            {
                if (const auto physicalSource = sourceCatalog->getPhysicalSource(bindUnsignedIntegerLiteral(physicalSourceSubject->id));
                    physicalSource.has_value())
                {
                    if (sourceCatalog->removePhysicalSource(physicalSource.value()))
                    {
                        return DropPhysicalSourceStatement{*physicalSource};
                    }
                    return DropPhysicalSourceStatement{
                        std::unexpected(UnregisteredSource("Physical source {} couldn't be unregistered", *physicalSource))};
                }
                throw UnregisteredSource("There is no physical source with id {}", physicalSourceSubject->id->getText());
            }
        }
        else if (const auto* const dropQueryAst = dropAst->dropSubject()->dropQuery(); dropQueryAst != nullptr)
        {
            const auto id = QueryId{std::stoul(dropQueryAst->id->getText())};
            return DropQueryStatement{id};
        }
    }
    if (auto* const queryAst = statementAST->query(); queryAst != nullptr)
    {
        return queryBinder(queryAst);
    }

    throw InvalidStatement(statementAST->toString());
}
Statement StatementBinder::parseAndBind(const std::string_view statementString)
{
    antlr4::ANTLRInputStream input(statementString.data(), statementString.length());
    AntlrSQLLexer lexer(&input);
    antlr4::CommonTokenStream tokens(&lexer);
    AntlrSQLParser parser(&tokens);
    /// Enable that antlr throws exeptions on parsing errors
    parser.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());
    AntlrSQLParser::StatementContext* tree = parser.statement();
    return bind(tree);
}
}