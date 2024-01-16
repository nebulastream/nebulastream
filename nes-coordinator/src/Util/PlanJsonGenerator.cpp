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

#include <Catalogs/Topology/TopologyNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/RenameSourceOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/WindowLogicalOperatorNode.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanJsonGenerator.hpp>

namespace NES {

std::string PlanJsonGenerator::getOperatorType(const OperatorNodePtr& operatorNode) {
    NES_INFO("Util: getting the type of the operator");

    std::string operatorType;
    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        if (operatorNode->as<SourceLogicalOperatorNode>()
                ->getSourceDescriptor()
                ->instanceOf<Network::NetworkSourceDescriptor>()) {
            operatorType = "SOURCE_SYS";
        } else {
            operatorType = "SOURCE";
        }
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        operatorType = "FILTER";
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        operatorType = "MAP";
    } else if (operatorNode->instanceOf<WindowLogicalOperatorNode>()) {
        operatorType = "WINDOW AGGREGATION";
    } else if (operatorNode->instanceOf<JoinLogicalOperatorNode>()) {
        operatorType = "JOIN";
    } else if (operatorNode->instanceOf<ProjectionLogicalOperatorNode>()) {
        operatorType = "PROJECTION";
    } else if (operatorNode->instanceOf<UnionLogicalOperatorNode>()) {
        operatorType = "UNION";
    } else if (operatorNode->instanceOf<RenameSourceOperatorNode>()) {
        operatorType = "RENAME";
    } else if (operatorNode->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        operatorType = "WATERMARK";
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        if (operatorNode->as<SinkLogicalOperatorNode>()->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>()) {
            operatorType = "SINK_SYS";
        } else {
            operatorType = "SINK";
        }
    } else {
        operatorType = "UNDEFINED";
    }
    NES_DEBUG("UtilityFunctions: operatorType =  {}", operatorType);
    return operatorType;
}

void PlanJsonGenerator::getChildren(OperatorNodePtr const& root,
                                    std::vector<nlohmann::json>& nodes,
                                    std::vector<nlohmann::json>& edges) {

    std::vector<nlohmann::json> childrenNode;

    std::vector<NodePtr> children = root->getChildren();
    if (children.empty()) {
        NES_DEBUG("UtilityFunctions::getChildren : children is empty()");
        return;
    }

    NES_DEBUG("UtilityFunctions::getChildren : children size =  {}", children.size());
    for (const NodePtr& child : children) {
        // Create a node JSON object for the current operator
        nlohmann::json node;
        auto childLogicalOperatorNode = child->as<LogicalOperatorNode>();
        std::string childOPeratorType = getOperatorType(childLogicalOperatorNode);

        // use the id of the current operator to fill the id field
        node["id"] = childLogicalOperatorNode->getId();

        if (childOPeratorType == "WINDOW AGGREGATION") {
            // window operator node needs more information, therefore we added information about window type and aggregation
            node["name"] = childLogicalOperatorNode->as<WindowLogicalOperatorNode>()->toString();
            NES_DEBUG("{}", childLogicalOperatorNode->as<WindowLogicalOperatorNode>()->toString());
        } else {
            // use concatenation of <operator type>(OP-<operator id>) to fill name field
            // e.g. FILTER(OP-1)
            node["name"] = childOPeratorType + "(OP-" + std::to_string(childLogicalOperatorNode->getId()) + ")";
        }
        node["nodeType"] = childOPeratorType;

        // store current node JSON object to the `nodes` JSON array
        nodes.push_back(node);

        // Create an edge JSON object for current operator
        nlohmann::json edge;

        if (childOPeratorType == "WINDOW AGGREGATION") {
            // window operator node needs more information, therefore we added information about window type and aggregation
            edge["source"] = childLogicalOperatorNode->as<WindowLogicalOperatorNode>()->toString();
        } else {
            edge["source"] = childOPeratorType + "(OP-" + std::to_string(childLogicalOperatorNode->getId()) + ")";
        }

        if (getOperatorType(root) == "WINDOW AGGREGATION") {
            edge["target"] = root->as<WindowLogicalOperatorNode>()->toString();
        } else {
            edge["target"] = getOperatorType(root) + "(OP-" + std::to_string(root->getId()) + ")";
        }
        // store current edge JSON object to `edges` JSON array
        edges.push_back(edge);

        // traverse to the children of current operator
        getChildren(childLogicalOperatorNode, nodes, edges);
    }
}

nlohmann::json PlanJsonGenerator::getExecutionPlanAsJson(const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan,
                                                         QueryId queryId) {
    NES_INFO("UtilityFunctions: getting execution plan as JSON");

    nlohmann::json executionPlanJson{};
    std::vector<nlohmann::json> nodes = {};

    auto executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    for (const Optimizer::ExecutionNodePtr& executionNode : executionNodes) {
        nlohmann::json currentExecutionNodeJsonValue{};

        currentExecutionNodeJsonValue["executionNodeId"] = executionNode->getId();
        currentExecutionNodeJsonValue["topologyNodeId"] = executionNode->getTopologyNode()->getId();
        currentExecutionNodeJsonValue["topologyNodeIpAddress"] = executionNode->getTopologyNode()->getIpAddress();

        auto allDecomposedQueryPlans = executionNode->getAllDecomposedQueryPlans(queryId);
        if (!allDecomposedQueryPlans.empty()) {
            continue;
        }

        nlohmann::json queryToQuerySubPlans{};
        queryToQuerySubPlans["queryId"] = queryId;

        std::vector<nlohmann::json> scheduledSubQueries;
        // loop over all query sub plans inside the current executionNode
        for (const auto& decomposedQueryPlan : allDecomposedQueryPlans) {

            // prepare json object to hold information on current query sub plan
            nlohmann::json currentQuerySubPlan{};

            // id of current query sub plan
            currentQuerySubPlan["querySubPlanId"] = decomposedQueryPlan->getDecomposedQueryPlanId();

            // add the string containing operator to the json object of current query sub plan
            currentQuerySubPlan["querySubPlan"] = decomposedQueryPlan->toString();

            scheduledSubQueries.push_back(currentQuerySubPlan);
        }
        queryToQuerySubPlans["querySubPlans"] = scheduledSubQueries;
        currentExecutionNodeJsonValue["ScheduledQueries"] = queryToQuerySubPlans;
        nodes.push_back(currentExecutionNodeJsonValue);
    }

    // add `executionNodes` JSON array to the final JSON result
    executionPlanJson["executionNodes"] = nodes;
    return executionPlanJson;
}

nlohmann::json PlanJsonGenerator::getQueryPlanAsJson(const QueryPlanPtr& queryPlan) {

    NES_DEBUG("UtilityFunctions: Getting the json representation of the query plan");

    nlohmann::json result{};
    std::vector<nlohmann::json> nodes{};
    std::vector<nlohmann::json> edges{};

    OperatorNodePtr root = queryPlan->getRootOperators()[0];

    if (!root) {
        NES_DEBUG("UtilityFunctions::getQueryPlanAsJson : root operator is empty");
        nlohmann::json node;
        node["id"] = "NONE";
        node["name"] = "NONE";
        nodes.push_back(node);
    } else {
        NES_DEBUG("UtilityFunctions::getQueryPlanAsJson : root operator is not empty");
        std::string rootOperatorType = getOperatorType(root);

        // Create a node JSON object for the root operator
        nlohmann::json node;

        // use the id of the root operator to fill the id field
        node["id"] = root->getId();

        // use concatenation of <operator type>(OP-<operator id>) to fill name field
        node["name"] = rootOperatorType + +"(OP-" + std::to_string(root->getId()) + ")";

        node["nodeType"] = rootOperatorType;

        nodes.push_back(node);

        // traverse to the children of the current operator
        getChildren(root, nodes, edges);
    }

    // add `nodes` and `edges` JSON array to the final JSON result
    result["nodes"] = nodes;
    result["edges"] = edges;
    return result;
}

}// namespace NES
