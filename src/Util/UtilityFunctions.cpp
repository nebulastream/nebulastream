#include <API/Pattern.hpp>
#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Operators/Operator.hpp>
#include <Plans/Query/QueryPlan.hpp>
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

QueryPtr UtilityFunctions::createQueryFromCodeString(const std::string& queryCodeSnippet) {
    bool pattern = queryCodeSnippet.find("Pattern::") != std::string::npos;
    bool merge = queryCodeSnippet.find(".merge") != std::string::npos;
    try {
        /* translate user code to a shared library, load and execute function, then return query object */
        std::stringstream code;
        code << "#include <API/Query.hpp>" << std::endl;
        code << "#include <API/Pattern.hpp>" << std::endl;
        code << "#include <API/Config.hpp>" << std::endl;
        code << "#include <API/Schema.hpp>" << std::endl;
        code << "#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>" << std::endl;
        code << "#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>" << std::endl;
        code << "#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>" << std::endl;
        code << "#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>" << std::endl;
        code << "#include <Sources/DataSource.hpp>" << std::endl;
        code << "#include <API/UserAPIExpression.hpp>" << std::endl;
        code << "#include <Catalogs/StreamCatalog.hpp>" << std::endl;
        code << "namespace NES{" << std::endl;
        code << "Query createQuery(){" << std::endl;

        std::string streamName = queryCodeSnippet.substr(
            queryCodeSnippet.find("::from("));
        streamName = streamName.substr(7, streamName.find(")") - 7);
        NES_DEBUG(" stream name = " << streamName);

        std::string newQuery = queryCodeSnippet;

        //if pattern
        if (pattern) {
            boost::replace_all(newQuery, "Pattern::from", "return Pattern::from");
        } else if (merge) {//if contains merge
            auto pos1 = queryCodeSnippet.find("merge(");
            std::string tmp = queryCodeSnippet.substr(pos1);
            auto pos2 = tmp.find(").");//find the end bracket of merge query
            std::string subquery = tmp.substr(6, pos2 - 6);
            NES_DEBUG("subquery = " << subquery);
            code << "auto subQuery = " << subquery << ";" << std::endl;
            newQuery.replace(pos1, pos2, "merge(&subQuery");
            NES_DEBUG("newQuery = " << newQuery);
            boost::replace_all(newQuery, "Query::from", "return Query::from");
        } else {
            // add return statement in front of input query
            // NOTE: This will not work if you have created object of Input query and do further manipulation
            boost::replace_all(newQuery, "Query::from", "return Query::from");
        }
        NES_DEBUG("newQuery = " << newQuery);
        code << newQuery << std::endl;
        code << "}" << std::endl;
        code << "}" << std::endl;
        NES_DEBUG(code.str());
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

        auto queryPtr = std::make_shared<Query>(query);
        std::string queryId = UtilityFunctions::generateIdString();
        queryPtr->getQueryPlan()->setQueryId(queryId);

        return queryPtr;
    } catch (std::exception& exc) {
        NES_ERROR(
            "UtilityFunctions: Failed to create the query from input code string: " << queryCodeSnippet
                                                                                    << exc.what());
        throw;
    } catch (...) {
        NES_ERROR(
            "UtilityFunctions: Failed to create the query from input code string: " << queryCodeSnippet);
        throw "Failed to create the query from input code string";
    }
}


SchemaPtr UtilityFunctions::createSchemaFromCode(const std::string& queryCodeSnippet) {
    try {
        /* translate user code to a shared library, load and execute function, then return query object */
        std::stringstream code;
        code << "#include <API/InputQuery.hpp>" << std::endl;
        code << "#include <API/Config.hpp>" << std::endl;
        code << "#include <API/Schema.hpp>" << std::endl;
        code << "#include <Sources/DataSource.hpp>" << std::endl;
        code << "#include <API/UserAPIExpression.hpp>" << std::endl;
        code << "#include <Catalogs/StreamCatalog.hpp>" << std::endl;
        code << "namespace NES{" << std::endl;

        code << "Schema createSchema(){" << std::endl;
        code << "return " << queryCodeSnippet << ";";
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
            "Failed to create the query from input code string: " << queryCodeSnippet);
        throw;
    } catch (...) {
        NES_ERROR(
            "Failed to create the query from input code string: " << queryCodeSnippet);
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

std::string UtilityFunctions::getFirstStringBetweenTwoDelimiters(const std::string& input, std::string s1, std::string s2) {
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
    std::vector<PhysicalTypePtr> types;
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (uint32_t i = 0; i < schema->getSize(); ++i) {
        auto physicalType = physicalDataTypeFactory.getPhysicalType(schema->get(i)->getDataType());
        offsets.push_back(physicalType->size());
        types.push_back(physicalType);
        NES_DEBUG("CodeGenerator: " + std::string("Field Size ") + schema->get(i)->toString() + std::string(": ") + std::to_string(physicalType->size()));
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
        str << schema->get(i)->name << ":" << physicalDataTypeFactory.getPhysicalType(schema->get(i)->dataType)->toString() << "|";
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

/**
 * @brief create CSV lines from the tuples
 * @param tbuffer the tuple buffer
 * @param schema how to read the tuples from the buffer
 * @return a full string stream as string
 */
std::string UtilityFunctions::printTupleBufferAsCSV(TupleBuffer& tbuffer, SchemaPtr schema) {
    std::stringstream ss;
    auto numberOfTuples = tbuffer.getNumberOfTuples();
    auto buffer = tbuffer.getBufferAs<char>();
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (size_t i = 0; i < numberOfTuples; i++) {
        size_t offset = 0;
        for (size_t j = 0; j < schema->getSize(); j++) {
            auto field = schema->get(j);
            auto ptr = field->getDataType();
            auto physicalType = physicalDataTypeFactory.getPhysicalType(ptr);
            auto fieldSize = physicalType->size();
            auto str = physicalType->convertRawToString(buffer + offset + i * schema->getSchemaSizeInBytes());
            ss << str.c_str();
            if (j < schema->getSize() - 1) {
                ss << ",";
            }
            offset += fieldSize;
        }
        ss << std::endl;
    }
    return ss.str();
}

void UtilityFunctions::findAndReplaceAll(std::string& data, std::string toSearch, std::string replaceStr) {
    // Get the first occurrence
    size_t pos = data.find(toSearch);
    // Repeat till end is reached
    while (pos != std::string::npos) {
        // Replace this occurrence of Sub String
        data.replace(pos, toSearch.size(), replaceStr);
        // Get the next occurrence from the current position
        pos = data.find(toSearch, pos + replaceStr.size());
    }
}

}// namespace NES
