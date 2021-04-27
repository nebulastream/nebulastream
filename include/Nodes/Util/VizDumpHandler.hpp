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

#ifndef NES_INCLUDE_NODES_UTIL_VIZ_DUMP_HANDLER_HPP_
#define NES_INCLUDE_NODES_UTIL_VIZ_DUMP_HANDLER_HPP_

#include <Nodes/Util/DumpHandler.hpp>
#include <memory>
#include <vector>
namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

namespace detail {
/**
 * @brief Data model to visualize edges in the nezviz format.
 */
class VizEdge {
  public:
    VizEdge(std::string id, std::string source, std::string target);
    std::string id;
    std::string source;
    std::string target;
    std::string serialize();
};

/**
 * @brief Data model to visualize nodes in the nezviz format.
 */
class VizNode {
  public:
    VizNode(std::string id, std::string label, std::string parent);
    VizNode(std::string id, std::string label);
    std::string id;
    std::string label;
    std::string parent;
    std::vector<std::tuple<std::string, std::string>> properties;
    void addProperty(std::tuple<std::string, std::string>);
    std::string serialize();
};

/**
 * @brief Data model to visualize query graphs in the nezviz format.
 */
class VizGraph {
  public:
    VizGraph(std::string name);
    std::string name;
    std::vector<VizEdge> edges;
    std::vector<VizNode> nodes;
    std::string serialize();
};
}// namespace detail

/**
 * @brief Dump handler to dump query plans to a viznez file.
 */
class VizDumpHandler : public DumpHandler {

  public:
    /**
     * @brief Creates a new VizDumpHandler.
     * Uses the current working directory as a storage locations.
     * @return DebugDumpHandlerPtr
     */
    static DebugDumpHandlerPtr create();

    /**
     * @brief Creates a new VizDumpHandler
     * @param rootDir directory for the viz dump files.
     */
    VizDumpHandler(std::string rootDir);

    void dump(const NodePtr node) override;

    /**
     * @brief Dump a query plan with a specific context and scope.
     * @param context the context
     * @param scope the scope
     * @param plan the query plan
     */
    void dump(std::string context, std::string scope, QueryPlanPtr queryPlan) override;

    /**
     * @brief Dump a pipeline query plan with a specific context and scope.
     * @param context the context
     * @param scope the scope
     * @param plan the query plan
     */
    void dump(std::string scope, std::string name, QueryCompilation::PipelineQueryPlanPtr ptr) override;

  private:
    void extractNodeProperties(detail::VizNode& node, OperatorNodePtr operatorNode);
    void dump(QueryPlanPtr queryPlan, std::string parent, detail::VizGraph& graph);
    void writeToFile(std::string scope, std::string name, std::string content);
    std::string rootDir;
};

}// namespace NES

#endif//NES_INCLUDE_NODES_UTIL_VIZ_DUMP_HANDLER_HPP_
