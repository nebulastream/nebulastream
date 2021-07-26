/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <Services/QueryParsingService.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/DynamicObject.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/SourceCode.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>
#include <sstream>

namespace NES {

QueryParsingService::QueryParsingService(std::shared_ptr<Compiler::JITCompiler> jitCompiler): jitCompiler(jitCompiler) {}

std::shared_ptr<QueryParsingService> QueryParsingService::create(std::shared_ptr<Compiler::JITCompiler> jitCompiler) {
    return std::make_shared<QueryParsingService>(jitCompiler);
}

SchemaPtr QueryParsingService::createSchemaFromCode(const std::string& queryCodeSnippet) {
    try {
        /* translate user code to a shared library, load and execute function, then return query object */
        std::stringstream code;
        code << "#include <API/Schema.hpp>" << std::endl;
        code << "#include <Sources/DataSource.hpp>" << std::endl;
        code << "namespace NES{" << std::endl;

        code << "Schema createSchema(){" << std::endl;
        code << "return " << queryCodeSnippet << ";";
        code << "}" << std::endl;
        code << "}" << std::endl;

        NES_DEBUG("generated code=" << code.str());
        auto sourceCode = std::make_unique<Compiler::SourceCode>("cpp", code.str());
        auto request = Compiler::CompilationRequest::create(std::move(sourceCode), "query", false, false, false, true);
        auto result = jitCompiler->compile(std::move(request));
        auto compiled_code = result.get().getDynamicObject();

        using CreateSchemaFunctionPtr = Schema (*)();
        auto func = compiled_code->getInvocableMember<CreateSchemaFunctionPtr>(
            "_ZN3NES12createSchemaEv");// was   _ZN5iotdb12createSchemaEv
        if (!func) {
            NES_ERROR("Error retrieving function! Symbol not found!");
        }
        /* call loaded function to create query object */
        Schema query((*func)());
        return std::make_shared<Schema>(query);

    } catch (std::exception& exc) {
        NES_ERROR("Failed to create the query from input code string: " << queryCodeSnippet);
        throw;
    } catch (...) {
        NES_ERROR("Failed to create the query from input code string: " << queryCodeSnippet);
        throw "Failed to create the query from input code string";
    }
}

QueryPtr QueryParsingService::createQueryFromCodeString(const std::string& queryCodeSnippet) {

    if (queryCodeSnippet.find("Stream(") != std::string::npos || queryCodeSnippet.find("Schema::create()") != std::string::npos) {
        NES_ERROR("QueryCatalog: queries are not allowed to specify schemas anymore.");
        throw InvalidQueryException("Queries are not allowed to define schemas anymore");
    }

    bool pattern = queryCodeSnippet.find("Pattern::") != std::string::npos;
    bool merge = queryCodeSnippet.find(".unionWith") != std::string::npos;
    try {
        /* translate user code to a shared library, load and execute function, then return query object */
        std::stringstream code;
        code << "#include <API/Query.hpp>" << std::endl;
        code << "#include <API/Pattern.hpp>" << std::endl;
        code << "#include <API/Schema.hpp>" << std::endl;
        code << "#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>" << std::endl;
        code << "#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>" << std::endl;
        code << "#include <Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>" << std::endl;
        code << "#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>" << std::endl;
        code << "#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>" << std::endl;
        code << "#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>" << std::endl;
        code << "#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>" << std::endl;
        code << "#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>" << std::endl;
        code << "#include <Sources/DataSource.hpp>" << std::endl;
        code << "using namespace NES::API;" << std::endl;
        code << "namespace NES{" << std::endl;
        code << "Query createQuery(){" << std::endl;

        std::string streamName = queryCodeSnippet.substr(queryCodeSnippet.find("::from("));
        streamName = streamName.substr(7, streamName.find(')') - 7);
        NES_DEBUG(" UtilityFunctions: stream name = " << streamName);

        std::string newQuery = queryCodeSnippet;

        if (merge) {//if contains merge
            auto pos1 = queryCodeSnippet.find("unionWith(");
            uint64_t closingLoc = findSubQueryTermination(pos1, queryCodeSnippet);
            std::string subquery = queryCodeSnippet.substr(pos1 + 10, closingLoc - pos1 - 10);
            NES_DEBUG("UtilityFunctions: subquery = " << subquery);
            code << "auto subQuery = " << subquery << ";" << std::endl;
            newQuery.replace(pos1, closingLoc - pos1, "unionWith(&subQuery");
            NES_DEBUG("UtilityFunctions: newQuery = " << newQuery);
        }

        // add return statement in front of input query/pattern
        //if pattern
        if (pattern) {
            UtilityFunctions::findAndReplaceAll(newQuery, "Pattern::from", "return Pattern::from");
        } else {// if Query
            newQuery = UtilityFunctions::replaceFirst(newQuery, "Query::from", "return Query::from");
        }

        NES_DEBUG("UtilityFunctions: parsed query = " << newQuery);
        code << newQuery << std::endl;
        code << "}" << std::endl;
        code << "}" << std::endl;
        NES_DEBUG("UtilityFunctions: query code \n" << code.str());
        auto sourceCode = std::make_unique<Compiler::SourceCode>("cpp", code.str());
        auto request = Compiler::CompilationRequest::create(std::move(sourceCode), "query", false, false, false, true);
        auto result = jitCompiler->compile(std::move(request));
        auto compiled_code = result.get().getDynamicObject();
        if (!code) {
            NES_ERROR("Compilation of query code failed! Code: " << code.str());
        }

        using CreateQueryFunctionPtr = Query (*)();
        auto func = compiled_code->getInvocableMember<CreateQueryFunctionPtr>("_ZN3NES11createQueryEv");
        if (!func) {
            NES_ERROR("UtilityFunctions: Error retrieving function! Symbol not found!");
        }
        /* call loaded function to create query object */
        Query query((*func)());

        return std::make_shared<Query>(query);
    } catch (std::exception& exc) {
        NES_ERROR("UtilityFunctions: Failed to create the query from input code string: " << queryCodeSnippet << exc.what());
        throw;
    } catch (...) {
        NES_ERROR("UtilityFunctions: Failed to create the query from input code string: " << queryCodeSnippet);
        throw "Failed to create the query from input code string";
    }
}

uint64_t QueryParsingService::findSubQueryTermination(uint64_t startOfUnionWith, const std::string& queryCodeSnippet) {
    uint64_t closingLoc = 0;
    uint64_t parenthesisCount = 0;
    //Iterate over the code string and identify the last location where the parenthesis are getting balanced
    for (uint64_t i = startOfUnionWith + 9; i < queryCodeSnippet.size(); i++) {
        if (queryCodeSnippet[i] == '(') {//When found open parenthesis, increment the parenthesis count
            parenthesisCount++;
        } else if (queryCodeSnippet[i] == ')') {//When found open parenthesis, decrement the parenthesis count
            parenthesisCount--;
        }

        if (parenthesisCount == 0) {//When found parenthesis count as zero then return the position
            closingLoc = i;
            break;
        }
    }
    //return the location where the parenthesis are getting balanced
    return closingLoc;
}

}// namespace NES::API