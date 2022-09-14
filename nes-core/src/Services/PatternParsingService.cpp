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
#include <Parsers/NebulaPSL/NebulaPSLOperatorNode.h>
#include <Parsers/NebulaPSL/NebulaPSLQueryPlanCreator.h>
#include <Parsers/NebulaPSL/gen/NesCEPLexer.h>
#include <Parsers/NebulaPSL/gen/NesCEPParser.h>
#include <Services/PatternParsingService.hpp>
#include <antlr4-runtime/ANTLRInputStream.h>

NES::QueryPtr NES::PatternParsingService::createPatternFromCodeString(const std::string& patternString) {
    NES_DEBUG("PatternParsingService: received the following pattern string" + patternString);
    // we hand over all auto-generated files (tokens, lexer, etc.) to ANTLR to create the AST
    antlr4::ANTLRInputStream input(patternString.c_str(), patternString.length());
    NesCEPLexer lexer(&input);
    antlr4::CommonTokenStream tokens(&lexer);
    NesCEPParser parser(&tokens);
    NesCEPParser::QueryContext* tree = parser.query();
    NES_DEBUG("PatternParsingService: ANTLR created the following AST from pattern string " + tree->toStringTree(&parser));

    NES_DEBUG("PatternParsingService: Parse the AST into a query plan");
    Parsers::NesCEPQueryPlanCreator queryPlanCreator;
    //The ParseTreeWalker performs a walk on the given AST starting at the root and going down recursively with depth-first search
    antlr4::tree::ParseTreeWalker::DEFAULT.walk(&queryPlanCreator, tree);
    auto query= queryPlanCreator.getQuery();
    NES_DEBUG("PatternParsingService: created the query from AST " + queryPlanCreator.getQuery().getQueryPlan()->toString());
    return std::make_shared<Query>(query);
}
