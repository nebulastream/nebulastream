#include <Catalogs/StreamCatalog.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/Compiler/CompiledCode.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <algorithm>
#include <boost/algorithm/string/replace.hpp>
#include <random>

namespace NES {

// removes leading and trailing whitespaces

std::string UtilityFunctions::trim(std::string s) {
    auto not_space = [](char c) {
        return isspace(c) == 0;
    };
    // trim left
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), not_space));
    // trim right
    s.erase(find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
    return s;
}

InputQueryPtr UtilityFunctions::createQueryFromCodeString(
    const std::string& query_code_snippet, StreamCatalogPtr streamCatalog) {
    try {
        /* translate user code to a shared library, load and execute function, then return query object */
        std::stringstream code;
        code << "#include <API/InputQuery.hpp>" << std::endl;
        code << "#include <API/Config.hpp>" << std::endl;
        code << "#include <API/Schema.hpp>" << std::endl;
        code << "#include <SourceSink/DataSource.hpp>" << std::endl;
        code << "#include <API/InputQuery.hpp>" << std::endl;
        code << "#include <API/UserAPIExpression.hpp>" << std::endl;
        code << "#include <Catalogs/StreamCatalog.hpp>" << std::endl;
        code << "namespace NES{" << std::endl;
        code << "InputQuery createQuery(StreamCatalogPtr streamCatalog){" << std::endl;

        //we will get the schema from the catalog, if stream does not exists this will through an exception
        std::string streamName = query_code_snippet.substr(
            query_code_snippet.find("::from("));
        streamName = streamName.substr(7, streamName.find(")") - 7);
        std::cout << " stream name = " << streamName << std::endl;
        code
            << "StreamPtr sPtr = streamCatalog->getStreamForLogicalStreamOrThrowException(\""
            << streamName << "\");";
        //code << "Stream& stream = *sPtr.get();" << std::endl;
        std::string newQuery = query_code_snippet;

        //replace the stream "xyz" provided by the user with the reference to the generated stream for the from clause
        boost::replace_all(newQuery, "from(" + streamName, "from(*sPtr.get()");

        //please the stream "xyz" provided by the user with the variable name of the generated stream for the writeToZmQ
        boost::replace_all(newQuery, "writeToZmq(" + streamName + ",",
                           "writeToZmq(\"" + streamName + "\",");

        //replace the stream "xyz" provided by the user with the variable name of the generated stream for the access inside the filter predicate
        boost::replace_all(newQuery, "filter(" + streamName,
                           "filter((*sPtr.get())");

        //replace the stream "xyz" provided by the user with the variable name of the generated stream for the access inside the filter predicate
        boost::replace_all(newQuery, "map(" + streamName, "map((*sPtr.get())");

        // add return statement in front of input query
        // NOTE: This will not work if you have created object of Input query and do further manipulation
        boost::replace_all(newQuery, "InputQuery::from", "return InputQuery::from");

        code << newQuery << std::endl;
        code << "}" << std::endl;
        code << "}" << std::endl;
        Compiler compiler;
        CompiledCodePtr compiled_code = compiler.compile(code.str(), true);
        if (!code) {
            NES_ERROR("Compilation of query code failed! Code: " << code.str());
        }

        //TODO: remove once the new API is in place
        InputQuery::streamCatalog = streamCatalog;

        typedef InputQuery (*CreateQueryFunctionPtr)(StreamCatalogPtr streamCatalog);
        CreateQueryFunctionPtr func = compiled_code->getFunctionPointer<CreateQueryFunctionPtr>(
            "_ZN3NES11createQueryESt10shared_ptrINS_13StreamCatalogEE");
        if (!func) {
            NES_ERROR("Error retrieving function! Symbol not found!");
        }
        /* call loaded function to create query object */
        InputQuery query((*func)(streamCatalog));

        return std::make_shared<InputQuery>(query);
    } catch (std::exception& exc) {
        NES_ERROR(
            "UtilityFunctions: Failed to create the query from input code string: " << query_code_snippet
                                                                                    << exc.what());
        throw;
    } catch (...) {
        NES_ERROR(
            "UtilityFunctions: Failed to create the query from input code string: " << query_code_snippet);
        throw "Failed to create the query from input code string";
    }
}

SchemaPtr UtilityFunctions::createSchemaFromCode(
    const std::string& query_code_snippet) {
    try {
        /* translate user code to a shared library, load and execute function, then return query object */
        std::stringstream code;
        code << "#include <API/InputQuery.hpp>" << std::endl;
        code << "#include <API/Config.hpp>" << std::endl;
        code << "#include <API/Schema.hpp>" << std::endl;
        code << "#include <SourceSink/DataSource.hpp>" << std::endl;
        code << "#include <API/InputQuery.hpp>" << std::endl;
        code << "#include <API/UserAPIExpression.hpp>" << std::endl;
        code << "#include <Catalogs/StreamCatalog.hpp>" << std::endl;
        code << "namespace NES{" << std::endl;

        code << "Schema createSchema(){" << std::endl;
        code << "return " << query_code_snippet << ";";
        code << "}" << std::endl;
        code << "}" << std::endl;
        Compiler compiler;
        CompiledCodePtr compiled_code = compiler.compile(code.str(), false);
        if (!code) {
            NES_ERROR("Compilation of schema code failed! Code: " << code.str());
        }

        typedef Schema (*CreateSchemaFunctionPtr)();
        CreateSchemaFunctionPtr func = compiled_code
                                           ->getFunctionPointer<CreateSchemaFunctionPtr>(
                                               "_ZN3NES12createSchemaEv");// was   _ZN5iotdb12createSchemaEv
        if (!func) {
            NES_ERROR("Error retrieving function! Symbol not found!");
        }
        /* call loaded function to create query object */
        Schema query((*func)());
        return std::make_shared<Schema>(query);

    } catch (std::exception& exc) {
        NES_ERROR(
            "Failed to create the query from input code string: " << query_code_snippet);
        throw;
    } catch (...) {
        NES_ERROR(
            "Failed to create the query from input code string: " << query_code_snippet);
        throw "Failed to create the query from input code string";
    }
}

std::string UtilityFunctions::generateQueryId() {
    static std::random_device              rd;
    static std::mt19937                    gen(rd());
    static std::uniform_int_distribution<> dis(0, 15);
    static std::uniform_int_distribution<> dis2(8, 11);

    std::stringstream ss;
    int i;
    ss << std::hex;
    for (i = 0; i < 8; i++) {
        ss << dis(gen);
    }
    ss << "-";
    for (i = 0; i < 4; i++) {
        ss << dis(gen);
    }
    ss << "-4";
    for (i = 0; i < 3; i++) {
        ss << dis(gen);
    }
    ss << "-";
    ss << dis2(gen);
    for (i = 0; i < 3; i++) {
        ss << dis(gen);
    }
    ss << "-";
    for (i = 0; i < 12; i++) {
        ss << dis(gen);
    };
    NES_DEBUG("UtilityFunctions: generateQueryId: " + ss.str());
    return ss.str();
}

std::string UtilityFunctions::generateId() {
    static std::random_device dev;
    static std::mt19937 rng(dev());

    std::uniform_int_distribution<int> dist(0, 15);

    const char *v = "0123456789abcdef";
    const bool dash[] = { 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0 };

    std::string res;
    for (int i = 0; i < 16; i++) {
        if (dash[i]) res += "-";
        res += v[dist(rng)];
        res += v[dist(rng)];
    }

    NES_DEBUG("UtilityFunctions: generateId: " + res);
    return res;

}

std::string UtilityFunctions::getFirstStringBetweenTwoDelimiters(
    const std::string& input, std::string s1, std::string s2) {
    unsigned firstDelimPos = input.find(s1);
    unsigned endPosOfFirstDelim = firstDelimPos + s1.length();

    unsigned lastDelimPos = input.find_first_of(s2, endPosOfFirstDelim);

    return input.substr(endPosOfFirstDelim, lastDelimPos - endPosOfFirstDelim);
}

}// namespace NES
