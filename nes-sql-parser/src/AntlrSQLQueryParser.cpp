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

#include <SQLQueryParser/AntlrSQLQueryParser.hpp>

#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <ANTLRInputStream.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <BailErrorStrategy.h>
#include <BaseErrorListener.h>
#include <CommonTokenStream.h>
#include <Exceptions.h>
#include <Lexer.h>
#include <Parser.h>
#include <Recognizer.h>
#include <Token.h>
#include <AntlrSQLParser/AntlrSQLQueryPlanCreator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES::AntlrSQLQueryParser
{

namespace
{
class ThrowingErrorListener final : public antlr4::BaseErrorListener
{
public:
    explicit ThrowingErrorListener(std::string_view query) : query(query) { }

    void syntaxError(
        antlr4::Recognizer*, antlr4::Token*, size_t line, size_t charPositionInLine, const std::string& msg, std::exception_ptr) override
    {
        throw InvalidQuerySyntax(
            "Antlr exception during parsing: {} in {}", fmt::format("line {}:{} {}", line, charPositionInLine, msg), query);
    }

private:
    std::string_view query;
};

/// Stops ANTLR from recovering and raises the exception without printing to std::cerr.
std::shared_ptr<antlr4::ANTLRErrorListener>
installErrorListenerAndHandler(std::string_view query, antlr4::Lexer& lexer, antlr4::Parser& parser)
{
    lexer.removeErrorListeners();
    parser.removeErrorListeners();
    auto listener = std::make_shared<ThrowingErrorListener>(query);
    lexer.addErrorListener(listener.get());
    parser.addErrorListener(listener.get());
    parser.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());
    return listener;
}
}

LogicalPlan bindLogicalQueryPlan(AntlrSQLParser::QueryContext* queryAst)
{
    try
    {
        Parsers::AntlrSQLQueryPlanCreator queryPlanCreator;
        antlr4::tree::ParseTreeWalker::DEFAULT.walk(&queryPlanCreator, queryAst);
        auto queryPlan = queryPlanCreator.getQueryPlan();
        NES_DEBUG("Created the following query from antlr AST: \n{}", queryPlan);
        return queryPlan;
    }
    catch (antlr4::RuntimeException& antlrException)
    {
        throw InvalidQuerySyntax("Antlr exception during parsing: {} in {}", antlrException.what(), queryAst->getText());
    }
}

LogicalPlan createLogicalQueryPlanFromSQLString(std::string_view queryString)
{
    try
    {
        antlr4::ANTLRInputStream input(queryString.data(), queryString.length());
        AntlrSQLLexer lexer(&input);
        antlr4::CommonTokenStream tokens(&lexer);
        AntlrSQLParser parser(&tokens);
        [[maybe_unused]] auto listener = installErrorListenerAndHandler(queryString, lexer, parser);
        AntlrSQLParser::QueryContext* tree = parser.query();
        Parsers::AntlrSQLQueryPlanCreator queryPlanCreator;
        antlr4::tree::ParseTreeWalker::DEFAULT.walk(&queryPlanCreator, tree);
        auto queryPlan = queryPlanCreator.getQueryPlan();
        queryPlan.setOriginalSql(std::string(queryString));
        // NES_DEBUG("Created the following query from antlr AST: \n{}", queryPlan);
        return queryPlan;
    }
    catch (antlr4::RuntimeException& antlrException)
    {
        throw InvalidQuerySyntax("Antlr exception during parsing: {} in {}", antlrException.what(), queryString);
    }
}

std::shared_ptr<ManagedAntlrParser> ManagedAntlrParser::create(std::string_view input)
{
    return std::make_shared<ManagedAntlrParser>(Private{}, input);
}

ManagedAntlrParser::ManagedAntlrParser(Private, const std::string_view input)
    : inputStream{input.data(), input.length()}, lexer{&inputStream}, tokens{&lexer}, parser(&tokens), originalQuery(input)
{
    errorListener = installErrorListenerAndHandler(originalQuery, lexer, parser);
}

std::expected<ManagedAntlrParser::ManagedContext<AntlrSQLParser::StatementContext>, Exception> ManagedAntlrParser::parseSingle()
{
    try
    {
        AntlrSQLParser::TerminatedStatementContext* tree = parser.terminatedStatement();
        return ManagedContext{tree->statement(), shared_from_this()};
    }
    catch (antlr4::RuntimeException& antlrException)
    {
        return std::unexpected{InvalidQuerySyntax("Antlr exception during parsing: {} in {}", antlrException.what(), originalQuery)};
    }
}

std::expected<std::vector<ManagedAntlrParser::ManagedContext<AntlrSQLParser::StatementContext>>, Exception>
ManagedAntlrParser::parseMultiple()
{
    try
    {
        AntlrSQLParser::MultipleStatementsContext* tree = parser.multipleStatements();
        return tree->statement()
            | std::views::transform([this](auto statement) { return ManagedContext{statement, this->shared_from_this()}; })
            | std::ranges::to<std::vector>();
    }
    catch (antlr4::RuntimeException& antlrException)
    {
        return std::unexpected{InvalidQuerySyntax("Antlr exception during parsing: {} in {}", antlrException.what(), originalQuery)};
    }
}
}
