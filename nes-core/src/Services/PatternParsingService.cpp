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
#include <ANTLRInputStream.h>
#include <Parsers/PSL/NePSLPattern.hpp>
#include <Parsers/PSL/NesCEPQueryPlanCreator.hpp>
#include <Parsers/PSL/gen/NesCEPLexer.h>
#include <Parsers/PSL/gen/NesCEPParser.h>
#include <Services/PatternParsingService.h>
#include <cstring>

NES::QueryPtr NES::PatternParsingService::createPatternFromCodeString(const std::string& patternString) {
    //ANTLR Pipeline
    antlr4::ANTLRInputStream input(patternString.c_str(), patternString.length());
    NesCEPLexer lexer(&input);
    antlr4::CommonTokenStream tokens(&lexer);
    NesCEPParser parser(&tokens);
    NesCEPParser::QueryContext* tree=parser.query();
    std::cout << tree->toStringTree(&parser) << std::endl;
    Parsers::NesCEPQueryPlanCreator queryPlanCreator;
    antlr4::tree::ParseTreeWalker::DEFAULT.walk(&queryPlanCreator, tree);
    auto pattern= queryPlanCreator.getQuery();
    return std::make_shared<Query>(pattern);
}
