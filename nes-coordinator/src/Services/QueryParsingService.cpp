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
#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/DynamicObject.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/SourceCode.hpp>
#include <Parsers/NebulaPSL/NebulaPSLQueryPlanCreator.hpp>
#include <Parsers/NebulaPSL/gen/NesCEPLexer.h>
#include <Services/QueryParsingService.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>
#include <sstream>

namespace NES {

QueryParsingService::QueryParsingService(std::shared_ptr<Compiler::JITCompiler> jitCompiler) : jitCompiler(jitCompiler) {}

std::shared_ptr<QueryParsingService> QueryParsingService::create(std::shared_ptr<Compiler::JITCompiler> jitCompiler) {
    return std::make_shared<QueryParsingService>(jitCompiler);
}

SchemaPtr QueryParsingService::createSchemaFromCode(const std::string& queryCodeSnippet) {
    try {
        /* translate user code to a shared library, load and execute function, then return query object */
        std::stringstream code;
        code << "#include <API/Schema.hpp>" << std::endl;
        code << "#include <Common/DataTypes/DataTypeFactory.hpp>" << std::endl;
        code << "namespace NES{" << std::endl;

        code << "Schema createSchema(){" << std::endl;
        code << "return " << queryCodeSnippet << ";";
        code << "}" << std::endl;
        code << "}" << std::endl;

        NES_DEBUG("QueryParsingService : generated code= {}", code.str());
        auto sourceCode = std::make_unique<Compiler::SourceCode>(Compiler::Language::CPP, code.str());
        auto request = Compiler::CompilationRequest::create(std::move(sourceCode), "query", false, false, false, false);
        auto result = jitCompiler->compile(std::move(request));
        auto compiled_code = result.get().getDynamicObject();

        using CreateSchemaFunctionPtr = Schema (*)();
        auto func = compiled_code->getInvocableMember<CreateSchemaFunctionPtr>(
            "_ZN3NES12createSchemaEv");// was   _ZN5iotdb12createSchemaEv
        if (!func) {
            NES_ERROR("QueryParsingService : Error retrieving function! Symbol not found!");
        }
        /* call loaded function to create query object */
        Schema query((*func)());
        return std::make_shared<Schema>(query);

    } catch (std::exception& exc) {
        NES_ERROR("QueryParsingService: Failed to create the query from input code string: {}", queryCodeSnippet);
        throw;
    } catch (...) {
        NES_ERROR("QueryParsingService : Failed to create the query from input code string: {}", queryCodeSnippet);
        throw "Failed to create the query from input code string";
    }
}

QueryPlanPtr QueryParsingService::createQueryFromCodeString(const std::string& queryCodeSnippet) {

    if (queryCodeSnippet.find("Source(") != std::string::npos || queryCodeSnippet.find("Schema::create()") != std::string::npos) {
        NES_ERROR("QueryParsingService: queryIdAndCatalogEntryMapping are not allowed to specify schemas anymore.");
        throw InvalidQueryException("Queries are not allowed to define schemas anymore");
    }

    try {
        /* translate user code to a shared library, load and execute function, then return query object */
        std::stringstream code;
        code << "#include <API/QueryAPI.hpp>" << std::endl;
        code << "namespace NES{" << std::endl;
        code << "Query createQuery(){" << std::endl;

        std::string sourceName = queryCodeSnippet.substr(queryCodeSnippet.find("::from("));
        sourceName = sourceName.substr(7, sourceName.find(')') - 7);
        NES_DEBUG(" QueryParsingService: source name = {}", sourceName);

        std::string newQuery = queryCodeSnippet;
        // add return statement in front of input query
        if (newQuery.find("Query::sink2") != std::string::npos) {
            newQuery = NES::Util::replaceFirst(newQuery, "Query::sink2", "return Query::sink2");
        } else {
            newQuery = NES::Util::replaceFirst(newQuery, "Query::from", "return Query::from");
        }

        NES_DEBUG("QueryParsingService: parsed query = {}", newQuery);
        code << newQuery << std::endl;
        code << "}" << std::endl;
        code << "}" << std::endl;
        NES_DEBUG("QueryParsingService: query code\n{}", code.str());
        auto sourceCode = std::make_unique<Compiler::SourceCode>(Compiler::Language::CPP, code.str());
        auto request = Compiler::CompilationRequest::create(std::move(sourceCode), "query", true, false, false, false);
        auto result = jitCompiler->compile(std::move(request));
        auto compiled_code = result.get().getDynamicObject();
        if (!code) {
            NES_ERROR("Compilation of query code failed! Code: {}", code.str());
        }

        using CreateQueryFunctionPtr = Query (*)();
        auto func = compiled_code->getInvocableMember<CreateQueryFunctionPtr>("_ZN3NES11createQueryEv");
        if (!func) {
            NES_ERROR("QueryParsingService: Error retrieving function! Symbol not found!");
        }
        /* call loaded function to create query object */
        Query query((*func)());

        return query.getQueryPlan();
    } catch (std::exception& exc) {
        NES_ERROR("QueryParsingService: Failed to create the query from input code string: {} {}", queryCodeSnippet, exc.what());
        throw;
    } catch (...) {
        NES_ERROR("QueryParsingService: Failed to create the query from input code string: {}", queryCodeSnippet);
        throw Exceptions::RuntimeException("Failed to create the query from input code string");
    }
}

QueryPlanPtr QueryParsingService::createPatternFromCodeString(const std::string& queryCodeSnippet) {
    // we hand over all auto-generated files (tokens, lexer, etc.) to ANTLR to create the AST
    if (queryCodeSnippet.empty() || queryCodeSnippet.size() < 15) {
        NES_THROW_RUNTIME_ERROR("QueryParsingService::createPatternFromCodeString: The query is too short, make sure you provide "
                                "at least a source (FROM) and a sink (INTO)");
    } else {
        antlr4::ANTLRInputStream input(queryCodeSnippet.c_str(), queryCodeSnippet.length());
        Parsers::NesCEPLexer lexer(&input);
        antlr4::CommonTokenStream tokens(&lexer);
        Parsers::NesCEPParser parser(&tokens);
        Parsers::NesCEPParser::QueryContext* tree = parser.query();
        NES_DEBUG("QueryParsingService: ANTLR created the following AST from pattern string {}", tree->toStringTree(&parser));

        NES_DEBUG("QueryParsingService: Parse the AST into a query plan");
        Parsers::NesCEPQueryPlanCreator queryPlanCreator;
        //The ParseTreeWalker performs a walk on the given AST starting at the root and going down recursively with depth-first search
        antlr4::tree::ParseTreeWalker::DEFAULT.walk(&queryPlanCreator, tree);
        auto queryPlan = queryPlanCreator.getQueryPlan();
        NES_DEBUG("PatternParsingService: created the query from AST {}", queryPlan->toString());
        return queryPlan;
    }
}

}// namespace NES
