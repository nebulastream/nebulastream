#include <API/Pattern.hpp>
#include <API/Query.hpp>
#include <Catalogs/QueryCatalogEntry.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
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

    if (queryCodeSnippet.find("Stream(") != std::string::npos || queryCodeSnippet.find("Schema::create()") != std::string::npos) {
        NES_ERROR("QueryCatalog: queries are not allowed to specify schemas anymore.");
        throw InvalidQueryException("Queries are not allowed to define schemas anymore");
    }

    bool pattern = queryCodeSnippet.find("Pattern::") != std::string::npos;
    bool merge = queryCodeSnippet.find(".merge") != std::string::npos;
    try {
        /* translate user code to a shared library, load and execute function, then return query object */
        std::stringstream code;
        code << "#include <API/Query.hpp>" << std::endl;
        code << "#include <API/Pattern.hpp>" << std::endl;
        code << "#include <API/Schema.hpp>" << std::endl;
        code << "#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>" << std::endl;
        code << "#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>" << std::endl;
        code << "#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>" << std::endl;
        code << "#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>" << std::endl;
        code << "#include <Sources/DataSource.hpp>" << std::endl;
        code << "#include <API/Window/WindowMeasure.hpp>" << std::endl;
        code << "#include <API/Window/WindowType.hpp>" << std::endl;
        code << "#include <API/Window/TimeCharacteristic.hpp>" << std::endl;
        code << "#include <API/Window/WindowAggregation.hpp>" << std::endl;
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

        return std::make_shared<Query>(query);
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
        code << "#include <API/Schema.hpp>" << std::endl;
        code << "#include <Sources/DataSource.hpp>" << std::endl;
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
std::string UtilityFunctions::printTupleBufferAsText(TupleBuffer& buffer) {
    std::stringstream ss;
    for (size_t i = 0; i < buffer.getNumberOfTuples(); i++) {
        ss << buffer.getBufferAs<char>()[i];
    }
    return ss.str();
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

const std::string UtilityFunctions::toCSVString(SchemaPtr schema) {
    std::stringstream ss;
    for (auto& f : schema->fields) {
        ss << f->toString() << ",";
    }
    ss.seekp(-1, std::ios_base::end);
    ss << std::endl;
    return ss.str();
}

bool UtilityFunctions::endsWith(const std::string& fullString, const std::string& ending) {
    if (fullString.length() >= ending.length()) {
        // get the start of the ending index of the full string and compare with the ending string
        return (0 == fullString.compare(fullString.length() - ending.length(), ending.length(), ending));
    } else {
        // if full string is smaller than the ending automatically return false
        return false;
    }
}

bool UtilityFunctions::startsWith(const std::string& fullString, const std::string& ending) {
    return (fullString.rfind(ending, 0) == 0);
}

uint64_t UtilityFunctions::getNextQueryId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

uint64_t UtilityFunctions::getNextOperatorId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

uint64_t UtilityFunctions::getNextQueryExecutionId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

uint64_t UtilityFunctions::getNextNodeId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}
web::json::value UtilityFunctions::getTopologyAsJson(TopologyNodePtr root) {
    web::json::value topologyJson{};

    std::deque<TopologyNodePtr> parentToPrint{root};
    std::deque<TopologyNodePtr> childToPrint;

    std::vector<web::json::value> nodes = {};
    std::vector<web::json::value> edges = {};


    while (!parentToPrint.empty()) {
        TopologyNodePtr nodeToPrint = parentToPrint.front();
        web::json::value currentNodeJsonValue{};

        parentToPrint.pop_front();
        currentNodeJsonValue["id"] = web::json::value::number(nodeToPrint->getId());
        currentNodeJsonValue["available_resources"] = web::json::value::number(nodeToPrint->getAvailableResources());
        currentNodeJsonValue["ip_address"] = web::json::value::string(nodeToPrint->getIpAddress());

        for (auto& child : nodeToPrint->getChildren()) {
            web::json::value currentEdgeJsonValue{};
            currentEdgeJsonValue["source"] = web::json::value::number(child->as<TopologyNode>()->getId());
            currentEdgeJsonValue["target"] = web::json::value::number(nodeToPrint->getId());
            edges.push_back(currentEdgeJsonValue);

            childToPrint.push_back(child->as<TopologyNode>());
        }

        if (parentToPrint.empty()) {
            parentToPrint.insert(parentToPrint.end(), childToPrint.begin(), childToPrint.end());
            childToPrint.clear();
        }

        nodes.push_back(currentNodeJsonValue);

    }

    topologyJson["nodes"] = web::json::value::array(nodes);
    topologyJson["edges"] = web::json::value::array(edges);
    return topologyJson;
}

std::string UtilityFunctions::getOperatorType(OperatorNodePtr operatorNode) {
    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        return "SOURCE";
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        return "FILTER";
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        return "MAP";
    } else if (operatorNode->instanceOf<MergeLogicalOperatorNode>()) {
        return "MERGE";
    } else if (operatorNode->instanceOf<WindowLogicalOperatorNode>()) {
        return "WINDOW";
    }  else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        return "SINK";
    }
    return std::string();
}

web::json::value UtilityFunctions::getQueryPlanAsJson(QueryCatalogPtr queryCatalog, QueryId queryId) {

    NES_INFO("QueryService: Get the registered query");
    if (!queryCatalog->queryExists(queryId)) {
        throw QueryNotFoundException("QueryService: Unable to find query with id " + std::to_string(queryId) + " in query catalog.");
    }
    QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);

    NES_INFO("QueryService: Getting the json representation of the query plan");

    web::json::value result{};
    std::vector<web::json::value> nodes{};
    std::vector<web::json::value> edges{};

    const OperatorNodePtr root = queryCatalogEntry->getQueryPlan()->getRootOperators()[0];

    if (!root) {
        auto node = web::json::value::object();
        node["id"] = web::json::value::string("NONE");
        node["title"] = web::json::value::string("NONE");
        nodes.push_back(node);
    } else {
        std::string rootOperatorType = getOperatorType(root);

        auto node = web::json::value::object();
        node["id"] = web::json::value::string(
            getOperatorType(root) + "(OP-" + std::to_string(root->getId()) + ")");
        node["title"] =
            web::json::value::string(
                rootOperatorType + +"(OP-" + std::to_string(root->getId()) + ")");
        if (rootOperatorType == "SOURCE" || rootOperatorType == "SINK") {
            node["nodeType"] = web::json::value::string("Source");
        } else {
            node["nodeType"] = web::json::value::string("Processor");
        }
        nodes.push_back(node);
        getQueryPlanChildren(root, nodes, edges);
    }

    result["nodes"] = web::json::value::array(nodes);
    result["edges"] = web::json::value::array(edges);

    return result;
}

void UtilityFunctions::getQueryPlanChildren(const OperatorNodePtr root, std::vector<web::json::value>& nodes,
                                        std::vector<web::json::value>& edges) {

    std::vector<web::json::value> childrenNode;

    std::vector<NodePtr> children = root->getChildren();
    if (children.empty()) {
        return;
    }

    for (NodePtr child : children) {
        auto node = web::json::value::object();
        auto childLogicalOperatorNode = child->as<LogicalOperatorNode>();
        std::string childOPeratorType = getOperatorType(childLogicalOperatorNode);

        node["id"] =
            web::json::value::string(childOPeratorType + "(OP-" + std::to_string(childLogicalOperatorNode->getId()) + ")");
        node["title"] =
            web::json::value::string(childOPeratorType + "(OP-" + std::to_string(childLogicalOperatorNode->getId()) + ")");

        if (childOPeratorType == "SOURCE"|| childOPeratorType == "SINK") {
            node["nodeType"] = web::json::value::string("Source");
        } else {
            node["nodeType"] = web::json::value::string("Processor");
        }

        nodes.push_back(node);

        auto edge = web::json::value::object();
        edge["source"] =
            web::json::value::string(
                childOPeratorType + "(OP-" + std::to_string(childLogicalOperatorNode->getId()) + ")");
        edge["target"] =
            web::json::value::string(
                getOperatorType(root) + "(OP-" + std::to_string(root->getId()) + ")");

        edges.push_back(edge);
        getQueryPlanChildren(childLogicalOperatorNode, nodes, edges);
    }
}

}// namespace NES
