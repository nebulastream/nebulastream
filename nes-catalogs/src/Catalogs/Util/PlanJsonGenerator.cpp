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
#include <Catalogs/Util/PlanJsonGenerator.hpp>
#include <Identifiers/NESStrongTypeJson.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/RenameSourceOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

std::string PlanJsonGenerator::getOperatorType(const OperatorPtr& operatorNode) {
    NES_TRACE("Util: getting the type of the operator");

    std::string operatorType;
    if (operatorNode->instanceOf<SourceLogicalOperator>()) {
        if (operatorNode->as<SourceLogicalOperator>()->getSourceDescriptor()->instanceOf<Network::NetworkSourceDescriptor>()) {
            operatorType = "SOURCE_SYS";
        } else {
            operatorType = "SOURCE";
        }
    } else if (operatorNode->instanceOf<LogicalFilterOperator>()) {
        operatorType = "FILTER";
    } else if (operatorNode->instanceOf<LogicalMapOperator>()) {
        operatorType = "MAP";
    } else if (operatorNode->instanceOf<LogicalWindowOperator>()) {
        operatorType = "WINDOW AGGREGATION";
    } else if (operatorNode->instanceOf<LogicalJoinOperator>()) {
        operatorType = "JOIN";
    } else if (operatorNode->instanceOf<LogicalProjectionOperator>()) {
        operatorType = "PROJECTION";
    } else if (operatorNode->instanceOf<LogicalUnionOperator>()) {
        operatorType = "UNION";
    } else if (operatorNode->instanceOf<RenameSourceOperator>()) {
        operatorType = "RENAME";
    } else if (operatorNode->instanceOf<WatermarkAssignerLogicalOperator>()) {
        operatorType = "WATERMARK";
    } else if (operatorNode->instanceOf<SinkLogicalOperator>()) {
        if (operatorNode->as<SinkLogicalOperator>()->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>()) {
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

void PlanJsonGenerator::getChildren(OperatorPtr const& root,
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
        auto childLogicalOperator = child->as<LogicalOperator>();
        std::string childOperatorType = getOperatorType(childLogicalOperator);

        // use the id of the current operator to fill the id field
        node["id"] = childLogicalOperator->getId();

        if (childOperatorType == "WINDOW AGGREGATION") {
            // window operator node needs more information, therefore we added information about window type and aggregation
            node["name"] = childLogicalOperator->as<LogicalWindowOperator>()->toString();
            NES_DEBUG("{}", childLogicalOperator->as<LogicalWindowOperator>()->toString());
        } else {
            // use concatenation of <operator type>(OP-<operator id>) to fill name field
            // e.g. FILTER(OP-1)
            node["name"] = fmt::format("{} (OP-{})", childOperatorType, childLogicalOperator->getId());
        }
        node["nodeType"] = childOperatorType;

        // store current node JSON object to the `nodes` JSON array
        nodes.push_back(node);

        // Create an edge JSON object for current operator
        nlohmann::json edge;

        if (childOperatorType == "WINDOW AGGREGATION") {
            // window operator node needs more information, therefore we added information about window type and aggregation
            edge["source"] = childLogicalOperator->as<LogicalWindowOperator>()->toString();
        } else {
            edge["source"] = fmt::format("{} (OP-{})", childOperatorType, childLogicalOperator->getId());
        }

        if (getOperatorType(root) == "WINDOW AGGREGATION") {
            edge["target"] = root->as<LogicalWindowOperator>()->toString();
        } else {
            edge["target"] = fmt::format("{} (OP-{})", getOperatorType(root), root->getId());
        }
        // store current edge JSON object to `edges` JSON array
        edges.push_back(edge);

        // traverse to the children of current operator
        getChildren(childLogicalOperator, nodes, edges);
    }
}

nlohmann::json PlanJsonGenerator::getQueryPlanAsJson(const QueryPlanPtr& queryPlan) {

    NES_DEBUG("UtilityFunctions: Getting the json representation of the query plan");

    nlohmann::json result{};
    std::vector<nlohmann::json> nodes{};
    std::vector<nlohmann::json> edges{};

    OperatorPtr root = queryPlan->getRootOperators()[0];

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
        node["name"] = fmt::format("{} (OP-{})", rootOperatorType, root->getId());

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
