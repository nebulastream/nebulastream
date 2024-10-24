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
#include <iostream>
#include <sstream>
#include <ANTLRInputStream.h>
#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <Parsers/NebulaSQL/NebulaSQLQueryPlanCreator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
namespace NES
{

QueryPlanPtr QueryParsingService::createQueryFromSQL(const std::string& query)
{
    try
    {
        antlr4::ANTLRInputStream input(query.c_str(), query.length());
        NebulaSQLLexer lexer(&input);
        antlr4::CommonTokenStream tokens(&lexer);
        NebulaSQLParser parser(&tokens);
        NebulaSQLParser::QueryContext* tree = parser.query();
        Parsers::NebulaSQLQueryPlanCreator queryPlanCreator;
        antlr4::tree::ParseTreeWalker::DEFAULT.walk(&queryPlanCreator, tree);
        auto queryPlan = queryPlanCreator.getQueryPlan();
        NES_DEBUG("Created the following query from antlr AST: \n{}", queryPlan->toString());
        return queryPlan;
    }
    catch (...)
    {
        throw InvalidQuerySyntax("Error during query parsing");
    }
}

}
