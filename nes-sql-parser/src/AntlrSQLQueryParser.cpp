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
#include <memory>
#include <string>
#include <string_view>
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
    catch (antlr4::RuntimeException antlrException)
    {
        throw InvalidQuerySyntax("Antlr exception during parsing: {} in {}", antlrException.what(), queryString);
    }
}

}
