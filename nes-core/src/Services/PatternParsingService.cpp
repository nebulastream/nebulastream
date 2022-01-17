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

#include <Services/PatternParsingService.h>
#include <API/NePSLPattern.h>
#include <cstring>
#include <ANTLRInputStream.h>
#include <Parsers/NePSL/gen/NesCEPLexer.h>
#include <Parsers/NePSL/gen/NesCEPParser.h>
#include <Parsers/NePSL/NesCEPQueryPlanCreator.h>

NES::QueryPtr NES::PatternParsingService::createPatternFromCodeString(const std::string& patternString) {

   int n = patternString.length();

   // declaring character array
   char char_array[n + 1];

   // copying the contents of the
   // string to char array
   strcpy(char_array, patternString.c_str());

   //ANTLR Pipeline
   antlr4::ANTLRInputStream input(char_array,n);
   NesCEPLexer lexer(&input);
   antlr4::CommonTokenStream tokens(&lexer);
   NesCEPParser parser(&tokens);
   NesCEPParser::QueryContext* tree=parser.query();
   std::cout << tree->toStringTree(&parser) << std::endl;
   NesCEPQueryPlanCreator queryPlanCreator;
   antlr4::tree::ParseTreeWalker::DEFAULT.walk(&queryPlanCreator, tree);
   auto pattern= queryPlanCreator.getQuery();
   return std::make_shared<Query>(pattern);
}
