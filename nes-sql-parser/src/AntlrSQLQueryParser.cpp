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
#include <CommonTokenStream.h>
#include <Exceptions.h>
#include <AntlrSQLParser/AntlrSQLQueryPlanCreator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::AntlrSQLQueryParser
{


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
        /// Enable that antlr throws exceptions on parsing errors
        parser.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());
        AntlrSQLParser::QueryContext* tree = parser.query();
        Parsers::AntlrSQLQueryPlanCreator queryPlanCreator;
        antlr4::tree::ParseTreeWalker::DEFAULT.walk(&queryPlanCreator, tree);
        auto queryPlan = queryPlanCreator.getQueryPlan();
        queryPlan.setOriginalSql(std::string(queryString));
        NES_DEBUG("Created the following query from antlr AST: \n{}", queryPlan);
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
    /// Enable that antlr throws exceptions on parsing errors
    parser.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());
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
