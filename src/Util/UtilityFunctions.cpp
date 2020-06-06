#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <NodeEngine/TupleBuffer.hpp>
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

QueryPtr UtilityFunctions::createQueryFromCodeString(
    const std::string& query_code_snippet) {
    try {
        /* translate user code to a shared library, load and execute function, then return query object */
        std::stringstream code;
        code << "#include <API/Query.hpp>" << std::endl;
        code << "#include <API/Config.hpp>" << std::endl;
        code << "#include <API/Schema.hpp>" << std::endl;
        code << "#include <API/Query.hpp>" << std::endl;
        code << "#include <Nodes/Operators/LogicalOperators/Sinks/CsvSinkDescriptor.hpp>" << std::endl;
        code << "#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>" << std::endl;
        code << "#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>" << std::endl;
        code << "#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>" << std::endl;
        code << "#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>" << std::endl;
        code << "#include <SourceSink/DataSource.hpp>" << std::endl;
        code << "#include <API/UserAPIExpression.hpp>" << std::endl;
        code << "#include <Catalogs/StreamCatalog.hpp>" << std::endl;
        code << "namespace NES{" << std::endl;
        code << "Query createQuery(){" << std::endl;

        std::string streamName = query_code_snippet.substr(
            query_code_snippet.find("::from("));
        streamName = streamName.substr(7, streamName.find(")") - 7);
        std::cout << " stream name = " << streamName << std::endl;
        std::string newQuery = query_code_snippet;

        // add return statement in front of input query
        // NOTE: This will not work if you have created object of Input query and do further manipulation
        boost::replace_all(newQuery, "Query::from", "return Query::from");

        code << newQuery << std::endl;
        code << "}" << std::endl;
        code << "}" << std::endl;
        Compiler compiler;
        CompiledCodePtr compiled_code = compiler.compile(code.str(), true);
        if (!code) {
            NES_ERROR("Compilation of query code failed! Code: " << code.str());
        }

        typedef Query (*CreateQueryFunctionPtr)();
        CreateQueryFunctionPtr func = compiled_code->getFunctionPointer<CreateQueryFunctionPtr>(
            "_ZN3NES11createQueryEv");
        if (!func) {
            NES_ERROR("Error retrieving function! Symbol not found!");
        }
        /* call loaded function to create query object */
        Query query((*func)());

        return std::make_shared<Query>(query);
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

std::string UtilityFunctions::generateIdString() {
    static std::random_device dev;
    static std::mt19937 rng(dev());

    std::uniform_int_distribution<int> dist(0, 15);

    const char* v = "0123456789abcdef";
    const bool dash[] = {0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0};

    std::string res;
    for (int i = 0; i < 16; i++) {
        if (dash[i])
            res += "-";
        res += v[dist(rng)];
        res += v[dist(rng)];
    }
    NES_DEBUG("UtilityFunctions: generateIdString: " + res);
    return res;
}

std::size_t UtilityFunctions::generateIdInt() {
    std::string linkID_string = UtilityFunctions::generateIdString();
    NES_DEBUG("UtilityFunctions: generateIdInt: create a new string_id=" << linkID_string);
    std::hash<std::string> hash_fn;
    return hash_fn(linkID_string);
}

std::string UtilityFunctions::getFirstStringBetweenTwoDelimiters(
    const std::string& input, std::string s1, std::string s2) {
    unsigned firstDelimPos = input.find(s1);
    unsigned endPosOfFirstDelim = firstDelimPos + s1.length();

    unsigned lastDelimPos = input.find_first_of(s2, endPosOfFirstDelim);

    return input.substr(endPosOfFirstDelim, lastDelimPos - endPosOfFirstDelim);
}

std::string UtilityFunctions::prettyPrintTupleBuffer(TupleBuffer& buffer, SchemaPtr schema) {
    if (!buffer.isValid()) {
        return "INVALID_BUFFER_PTR";
    }
    std::stringstream str;
    std::vector<uint32_t> offsets;
    std::vector<DataTypePtr> types;
    for (uint32_t i = 0; i < schema->getSize(); ++i) {
        offsets.push_back(schema->get(i)->getFieldSize());
        NES_DEBUG("CodeGenerator: " + std::string("Field Size ") + schema->get(i)->toString() + std::string(": ") + std::to_string(schema->get(i)->getFieldSize()));
        types.push_back(schema->get(i)->getDataType());
    }

    uint32_t prefix_sum = 0;
    for (uint32_t i = 0; i < offsets.size(); ++i) {
        uint32_t val = offsets[i];
        offsets[i] = prefix_sum;
        prefix_sum += val;
        NES_DEBUG("CodeGenerator: " + std::string("Prefix Sum: ") + schema->get(i)->toString() + std::string(": ") + std::to_string(offsets[i]));
    }

    str << "+----------------------------------------------------+" << std::endl;
    str << "|";
    for (uint32_t i = 0; i < schema->getSize(); ++i) {
        str << schema->get(i)->toString() << "|";
    }
    str << std::endl;
    str << "+----------------------------------------------------+" << std::endl;

    auto buf = buffer.getBufferAs<char>();
    for (uint32_t i = 0; i < buffer.getNumberOfTuples() * schema->getSchemaSizeInBytes();
         i += schema->getSchemaSizeInBytes()) {
        str << "|";
        for (uint32_t s = 0; s < offsets.size(); ++s) {
            void* value = &buf[i + offsets[s]];
            std::string tmp = types[s]->convertRawToString(value);
            str << tmp << "|";
        }
        str << std::endl;
    }
    str << "+----------------------------------------------------+";
    return str.str();
}

}// namespace NES
