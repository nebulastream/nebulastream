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

#include <Nodes/Node.hpp>
#include <Nodes/Util/VizDumpHandler.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Util/UtilityFunctions.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <utility>
#include <boost/stacktrace.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include "Nodes/Expressions/LogicalExpressions/LogicalBinaryExpressionNode.hpp"
#include "Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp"
#include "Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp"
#include "Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp"
#include "Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp"
#include "Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp"
#include "Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp"
#include "Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp"

namespace NES {

detail::VizGraph::VizGraph(std::string name) : name(std::move(name)) {}

std::string detail::VizGraph::serialize() {
    std::stringstream ss;
    ss << "{";
    ss << "\"nodes\": [";
    for (auto& node : nodes) {
        ss << node.serialize();
        if (&nodes.back() != &node) {
            ss << ",";
        }
    }
    ss << "],";
    ss << "\"edges\": [";
    for (auto& edge : edges) {
        ss << edge.serialize();
        if (&edges.back() != &edge) {
            ss << ",";
        }
    }
    ss << "]";
    ss << "}";
    return ss.str();
}

detail::VizNode::VizNode(std::string id, std::string label, std::string parent)
    : id(std::move(id)), label(std::move(label)), parent(std::move(parent)) {}

void detail::VizNode::addProperty(const std::tuple<std::string, std::string, std::string>& item) { properties.emplace_back(item); }

detail::VizNode::VizNode(std::string id, std::string label) : VizNode(std::move(id), std::move(label), "") {}

std::string detail::VizNode::serialize() {
    std::stringstream ss;
    ss << "{ \"data\": "
          "{\"id\": \""
       << id
       << "\","
          "\"label\":\""
       << label << "\",";
    if (!parent.empty()) {
        ss << R"("parent":")" << parent << "\",";
    }
    ss << "\"properties\":[";
    for (auto& tuple : properties) {
        // do this because otherwise the quotation marks are in bytes
        if (std::get<0>(tuple) == "FilterPredicate") {
            ss << "{\"" << std::get<0>(tuple) << "\":" << std::get<1>(tuple) << ", \"type\" :\"" << std::get<2>(tuple) << "\"}";
        } else {
            auto quotedValue = Util::escapeJson(std::get<1>(tuple));
            ss << "{\"" << std::get<0>(tuple) << "\":\"" << quotedValue << "\", \"type\" :\"" << std::get<2>(tuple) << "\"}";
        }
        if (&properties.back() != &tuple) {
            ss << ",";
        }
    }
    ss << "]";
    ss << "}}";
    return ss.str();
}

detail::VizEdge::VizEdge(std::string id, std::string source, std::string target)
    : id(std::move(id)), source(std::move(source)), target(std::move(target)) {}

std::string detail::VizEdge::serialize() const {
    std::stringstream ss;
    ss << "{ \"data\": "
          "{\"id\": \""
       << id
       << "\","
          "\"source\":\""
       << source << "\","
       << R"("target":")" << target << "\"}}";
    return ss.str();
}

VizDumpHandler::VizDumpHandler(std::string rootDir) : rootDir(std::move(rootDir)) {}

DebugDumpHandlerPtr VizDumpHandler::create() {
    std::string path = std::filesystem::current_path();
    path = path + std::filesystem::path::preferred_separator + "dump";
    if (!std::filesystem::is_directory(path)) {
        std::filesystem::create_directory(path);
    }
    return std::make_shared<VizDumpHandler>(path);
}

void VizDumpHandler::dump(const NodePtr) { NES_NOT_IMPLEMENTED(); }

void VizDumpHandler::dump(std::string context, std::string scope, QueryPlanPtr queryPlan) {
    NES_DEBUG("Dump query plan: " << queryPlan->getQueryId() << " : " << queryPlan->getQuerySubPlanId() << " for context "
                                  << context << " and scope " << scope);
    auto graph = detail::VizGraph("graph");
    dump(queryPlan, "", graph);
    writeToFile(context, scope, graph.serialize());
}

void VizDumpHandler::dump(QueryPlanPtr queryPlan, const std::string& parent, detail::VizGraph& graph) {
    auto queryPlanIter = QueryPlanIterator(std::move(queryPlan));
    for (auto&& op : queryPlanIter) {
        auto operatorNode = op->as<OperatorNode>();
        auto vizNode = detail::VizNode(std::to_string(operatorNode->getId()), op->toString(), parent);
        extractNodeProperties(vizNode, operatorNode);
        graph.nodes.emplace_back(vizNode);
        for (const auto& child : operatorNode->getChildren()) {
            auto childOperator = child->as<OperatorNode>();
            auto edgeId = std::to_string(operatorNode->getId()) + "_" + std::to_string(childOperator->getId());
            auto vizEdge = detail::VizEdge(edgeId, std::to_string(childOperator->getId()), std::to_string(operatorNode->getId()));
            graph.edges.emplace_back(vizEdge);
        }
    }
}

void VizDumpHandler::dump(std::string scope, std::string name, QueryCompilation::PipelineQueryPlanPtr pipelinePlan) {
    NES_DEBUG("Dump query plan: " << pipelinePlan->getQueryId() << " : " << pipelinePlan->getQuerySubPlanId() << " for scope "
                                  << scope);
    auto graph = detail::VizGraph("graph");
    for (const auto& pipeline : pipelinePlan->getPipelines()) {
        auto currentId = "p_" + std::to_string(pipeline->getPipelineId());
        auto vizNode = detail::VizNode(currentId, "Pipeline");
        graph.nodes.emplace_back(vizNode);
        dump(pipeline->getQueryPlan(), currentId, graph);
        for (const auto& successor : pipeline->getSuccessors()) {
            auto successorId = "p_" + std::to_string(successor->getPipelineId());
            auto edgeId = currentId + "_" + successorId;
            auto vizEdge = detail::VizEdge(edgeId, successorId,  currentId);
            graph.edges.emplace_back(vizEdge);
        }
    }
    writeToFile(scope, name, graph.serialize());
}

void VizDumpHandler::writeToFile(const std::string& scope, const std::string& name, const std::string& content) {
    dumpAsMap.insert(std::pair<std::string, std::string>(name, content));

    std::ofstream outputFile;
    auto scopeDir = rootDir + std::filesystem::path::preferred_separator + scope;
    auto fileName = scopeDir + std::filesystem::path::preferred_separator + name + ".nesviz";

    if (!std::filesystem::is_directory(scopeDir)) {
        std::filesystem::create_directory(scopeDir);
    }

    outputFile.open(fileName);
    outputFile << content;
    outputFile.close();
}
void VizDumpHandler::extractNodeProperties(detail::VizNode& node, const OperatorNodePtr& operatorNode) {
    node.addProperty({"StackTrace", operatorNode->getNodeSourceLocation(), "stacktrace"});
    // node.addProperty({"StackTrace", to_string(boost::stacktrace::stacktrace())});
    node.addProperty({"ClassName", operatorNode->getClassName(), ""});
    if (operatorNode->instanceOf<QueryCompilation::ExecutableOperator>()) {
        auto executableOperator = operatorNode->as<QueryCompilation::ExecutableOperator>();
        auto code = executableOperator->getExecutablePipelineStage()->getCodeAsString();
        node.addProperty({"OperatorCode", code, "code"});
    } else if (operatorNode->instanceOf<UnaryOperatorNode>()) {
        auto unaryOperator = operatorNode->as<UnaryOperatorNode>();
        node.addProperty({"InputSchema", unaryOperator->getInputSchema()->toString(), ""});
        node.addProperty({"OutputSchema", unaryOperator->getOutputSchema()->toString(), ""});

        if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
            auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
            auto expressionNode = filterOperator->getPredicate();
            std::map<uint64_t, std::string> mapOfPredicateNodes;
            std::vector<detail::VizEdge> edges;
            getFilterPredicatePreOrder(expressionNode, mapOfPredicateNodes, edges, 0);

            // showing contents:
            NES_DEBUG("VizDumpHandler::extractNodeProperties getPredicate Check");
            std::stringstream ss;
            ss << "{";
            ss << "\"nodes\": [";
            std::map<uint64_t, std::string>::iterator it = mapOfPredicateNodes.begin();
            for (it=mapOfPredicateNodes.begin(); it!=mapOfPredicateNodes.end(); ++ it){
                ss << R"({ "id": ")" << it->first << R"(", "label": ")" << it->second << "\"}, ";
            }
            ss.seekp(-1,ss.cur); ss << "], ";

            ss << "\"edges\": [";
            for (auto& edge : edges) {
                ss << edge.serialize();
                if (&edges.back() != &edge) {
                    ss << ",";
                }
            }
            ss << "]}";

            NES_DEBUG("VizDumpHandler::extractNodeProperties check FilterPredicate" << ss.str());
            node.addProperty({"FilterPredicate", ss.str(), ""});
        }
    }
}

void VizDumpHandler::getFilterPredicatePreOrder(ExpressionNodePtr node, std::map<uint64_t, std::string>& mapOfNodes, std::vector<detail::VizEdge>& edges, uint64_t parentId) {
    std::string nodeAsString = getLogicalBinaryExpression(node);
    NES_DEBUG("VizDumpHandler::getFilterPredicatePreOrder add node to map " << nodeAsString);

    uint64_t nodeId = mapOfNodes.size() + 1;

    if (node->instanceOf<BinaryExpressionNode>()){
        auto binaryExpression = node->as<BinaryExpressionNode>();
        mapOfNodes.insert(std::pair<uint64_t, std::string>((nodeId), nodeAsString));
        if (parentId > 0) {
            createPredicateEdge(std::to_string(nodeId), std::to_string(parentId), edges);
        }

        if (binaryExpression->getLeft()->instanceOf<ExpressionNode>()) {
            NES_DEBUG("VizDumpHandler::getFilterPredicatePreOrder retrieving left leaf ");
            getFilterPredicatePreOrder(binaryExpression->getLeft(), mapOfNodes, edges, nodeId);
        }

        if (binaryExpression->getRight()->instanceOf<ExpressionNode>()) {
            NES_DEBUG("VizDumpHandler::getFilterPredicatePreOrder retrieving right leaf");
            getFilterPredicatePreOrder(binaryExpression->getRight(), mapOfNodes, edges, nodeId);
        }
    } else {
        mapOfNodes.insert(std::pair<uint64_t, std::string>((nodeId), nodeAsString));
        createPredicateEdge(std::to_string(nodeId), std::to_string(parentId), edges);
    }
}

void VizDumpHandler::createPredicateEdge(std::string currentId, std::string parentId, std::vector<detail::VizEdge>& edges) {
    // target_source
    std::string edgeId = parentId + "_" + currentId;
    auto vizEdge = detail::VizEdge(edgeId, currentId,  parentId);
    edges.emplace_back(vizEdge);
}

std::string VizDumpHandler::getLogicalBinaryExpression(ExpressionNodePtr expressionNode) {
    if (expressionNode->instanceOf<AndExpressionNode>()) {
        return "&&";
    } else if (expressionNode->instanceOf<EqualsExpressionNode>()) {
        return "==";
    } else if (expressionNode->instanceOf<GreaterEqualsExpressionNode>()) {
        return ">=";
    } else if (expressionNode->instanceOf<GreaterExpressionNode>()) {
        return ">";
    } else if (expressionNode->instanceOf<LessEqualsExpressionNode>()) {
        return "<=";
    } else if (expressionNode->instanceOf<LessExpressionNode>()) {
        return "<";
    } else if (expressionNode->instanceOf<OrExpressionNode>()) {
        return "||";
    } else {
        return expressionNode->toString();
    }
}

std::map<std::string, std::string> VizDumpHandler::getDumpAsMap() {
    return dumpAsMap;
}

}// namespace NES
