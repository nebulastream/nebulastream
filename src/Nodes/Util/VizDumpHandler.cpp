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
#include <QueryCompiler/Operators/PhysicalOperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <filesystem>
#include <iostream>

namespace NES {

VizGraph::VizGraph(std::string name) : name(name) {}

std::string VizGraph::serialize() {
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

VizNode::VizNode(std::string id, std::string label, std::string parent) : id(id), label(label), parent(parent) {}

VizNode::VizNode(std::string id, std::string label) : VizNode(id, label, "") {}

std::string VizNode::serialize() {
    std::stringstream ss;
    if (parent.empty()) {
        ss << "{ \"data\": "
              "{\"id\": \""
           << id
           << "\","
              "\"label\":\""
           << label << "\"}}";

    } else {
        ss << "{ \"data\": "
              "{\"id\": \""
           << id
           << "\","
              "\"label\":\""
           << label << "\","
           << "\"parent\":\"" << parent << "\"}}";
    }
    return ss.str();
}

VizEdge::VizEdge(std::string id, std::string source, std::string target) : id(id), source(source), target(target) {}

std::string VizEdge::serialize() {
    std::stringstream ss;
    ss << "{ \"data\": "
          "{\"id\": \""
       << id
       << "\","
          "\"source\":\""
       << source << "\","
       << "\"target\":\"" << target << "\"}}";
    return ss.str();
}

VizDumpHandler::VizDumpHandler(std::string rootDir) : DumpHandler(),  rootDir(rootDir) {}

DebugDumpHandlerPtr VizDumpHandler::create() {
    std::string path = std::filesystem::current_path();
    path = path + std::filesystem::path::preferred_separator + "dump";
    if (!std::filesystem::is_directory(path)) {
        std::filesystem::create_directory(path);
    }
    return std::make_shared<VizDumpHandler>(path);
}

void VizDumpHandler::multilineDump(const NodePtr, std::ostream&) {}

void VizDumpHandler::dump(const NodePtr, std::ostream&) {}

void VizDumpHandler::dump(QueryPlanPtr queryPlan) {
    auto graph = VizGraph("graph");
    dump(queryPlan, "", graph);
    std::cout << graph.serialize() << std::endl;
}

void VizDumpHandler::dump(QueryPlanPtr queryPlan, std::string parent, VizGraph& graph) {
    auto queryPlanIter = QueryPlanIterator(queryPlan);

    for (auto op : queryPlanIter) {
        auto operatorNode = op->as<OperatorNode>();
        auto vizNode = VizNode(std::to_string(operatorNode->getId()), op->toString(), parent);
        graph.nodes.emplace_back(vizNode);
        for (auto child : operatorNode->getChildren()) {
            auto childOperator = child->as<OperatorNode>();
            auto edgeId = std::to_string(operatorNode->getId()) + "_" + std::to_string(childOperator->getId());
            auto vizEdge = VizEdge(edgeId, std::to_string(operatorNode->getId()), std::to_string(childOperator->getId()));
            graph.edges.emplace_back(vizEdge);
        }
    }
}

void VizDumpHandler::dump(std::string scope,
                          std::string name,
                          QueryCompilation::PipelineQueryPlanPtr pipelinePlan) {
    auto graph = VizGraph("graph");
    for (auto pipeline : pipelinePlan->getPipelines()) {
        auto currentId = "p_" + std::to_string(pipeline->getPipelineId());
        auto vizNode = VizNode(currentId, "Pipeline");
        graph.nodes.emplace_back(vizNode);
        dump(pipeline->getRootOperator(), currentId, graph);
        for (auto successor : pipeline->getSuccessors()) {
            auto successorId = "p_" + std::to_string(successor->getPipelineId());
            auto edgeId = currentId + "_" + successorId;
            auto vizEdge = VizEdge(edgeId, currentId, successorId);
            graph.edges.emplace_back(vizEdge);
        }
    }
    writeToFile(scope, name, graph.serialize());
}

void VizDumpHandler::writeToFile(std::string scope, std::string name, std::string content) {
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

}// namespace NES
