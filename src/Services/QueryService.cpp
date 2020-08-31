#include <Catalogs/QueryCatalog.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <Util/UtilityFunctions.hpp>
#include <WorkQueues/QueryRequestQueue.hpp>

namespace NES {

QueryService::QueryService(QueryCatalogPtr queryCatalog, QueryRequestQueuePtr queryRequestQueue) : queryCatalog(queryCatalog), queryRequestQueue(queryRequestQueue) {
    NES_DEBUG("QueryService()");
}

QueryService::~QueryService() {
    NES_DEBUG("~QueryService()");
}

uint64_t QueryService::validateAndQueueAddRequest(std::string queryString, std::string placementStrategyName) {

    NES_INFO("QueryService: Validating and registering the user query.");
    if (stringToPlacementStrategyType.find(placementStrategyName) == stringToPlacementStrategyType.end()) {
        NES_ERROR("QueryService: Unknown placement strategy name: " + placementStrategyName);
        throw InvalidArgumentException("placementStrategyName", placementStrategyName);
    }
    NES_INFO("QueryService: Parsing and converting user query string");
    QueryPtr query = UtilityFunctions::createQueryFromCodeString(queryString);
    QueryId queryId = UtilityFunctions::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    NES_INFO("QueryService: Queuing the query for the execution");
    QueryCatalogEntryPtr entry = queryCatalog->addNewQueryRequest(queryString, queryPlan, placementStrategyName);
    if (entry) {
        queryRequestQueue->add(entry);
        return queryId;
    } else {
        throw Exception("QueryService: unable to create query catalog entry");
    }
}

bool QueryService::validateAndQueueStopRequest(QueryId queryId) {

    NES_INFO("QueryService : stopping query " + queryId);
    if (!queryCatalog->queryExists(queryId)) {
        throw QueryNotFoundException("QueryService: Unable to find query with id " + std::to_string(queryId) + " in query catalog.");
    }
    QueryCatalogEntryPtr entry = queryCatalog->addQueryStopRequest(queryId);
    if (entry) {
        return queryRequestQueue->add(entry);
    }
    return false;
}

json::value QueryService::getQueryPlanAsJson(QueryId queryId) {

    NES_INFO("QueryService: Get the registered query");
    if (!queryCatalog->queryExists(queryId)) {
        throw QueryNotFoundException("QueryService: Unable to find query with id " + std::to_string(queryId) + " in query catalog.");
    }
    QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);

    NES_INFO("QueryService: Getting the json representation of the query plan");

    json::value result{};
    std::vector<json::value> nodes{};
    std::vector<json::value> edges{};

    const OperatorNodePtr root = queryCatalogEntry->getQueryPlan()->getRootOperators()[0];

    if (!root) {
        auto node = json::value::object();
        node["id"] = json::value::string("NONE");
        node["title"] = json::value::string("NONE");
        nodes.push_back(node);
    } else {
        std::string rootOperatorType = getOperatorType(root);

        auto node = json::value::object();
        node["id"] = json::value::string(
            getOperatorType(root) + "(OP-" + std::to_string(root->getId()) + ")");
        node["title"] =
            json::value::string(
                rootOperatorType + +"(OP-" + std::to_string(root->getId()) + ")");
        if (rootOperatorType == "SOURCE" || rootOperatorType == "SINK") {
            node["nodeType"] = json::value::string("Source");
        } else {
            node["nodeType"] = json::value::string("Processor");
        }
        nodes.push_back(node);
        getQueryPlanChildren(root, nodes, edges);
    }

    result["nodes"] = json::value::array(nodes);
    result["edges"] = json::value::array(edges);

    return result;
}

void QueryService::getQueryPlanChildren(const OperatorNodePtr root, std::vector<json::value>& nodes,
                                   std::vector<json::value>& edges) {

    std::vector<json::value> childrenNode;

    std::vector<NodePtr> children = root->getChildren();
    if (children.empty()) {
        return;
    }

    for (NodePtr child : children) {
        auto node = json::value::object();
        auto childLogicalOperatorNode = child->as<LogicalOperatorNode>();
        std::string childOPeratorType = getOperatorType(childLogicalOperatorNode);

        node["id"] =
            json::value::string(childOPeratorType + "(OP-" + std::to_string(childLogicalOperatorNode->getId()) + ")");
        node["title"] =
            json::value::string(childOPeratorType + "(OP-" + std::to_string(childLogicalOperatorNode->getId()) + ")");

        if (childOPeratorType == "SOURCE"|| childOPeratorType == "SINK") {
            node["nodeType"] = json::value::string("Source");
        } else {
            node["nodeType"] = json::value::string("Processor");
        }

        nodes.push_back(node);

        auto edge = json::value::object();
        edge["source"] =
            json::value::string(
                childOPeratorType + "(OP-" + std::to_string(childLogicalOperatorNode->getId()) + ")");
        edge["target"] =
            json::value::string(
                getOperatorType(root) + "(OP-" + std::to_string(root->getId()) + ")");

        edges.push_back(edge);
        getQueryPlanChildren(childLogicalOperatorNode, nodes, edges);
    }
}
std::string QueryService::getOperatorType(OperatorNodePtr operatorNode) {
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

}// namespace NES
