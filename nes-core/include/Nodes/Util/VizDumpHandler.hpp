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

#ifndef NES_INCLUDE_NODES_UTIL_VIZDUMPHANDLER_HPP_
#define NES_INCLUDE_NODES_UTIL_VIZDUMPHANDLER_HPP_

#include <Nodes/Util/DumpHandler.hpp>
#include <memory>
#include <vector>
namespace NES {

class Node;
using NodePtr = std::shared_ptr<Node>;

namespace detail {
/**
 * @brief Data model to visualize edges in the nezviz format.
 */
class VizEdge {
  public:
    /**
     * @brief Creates a new viz edge object, which represents a edge in the nezviz format
     * @param id edge id
     * @param source source id
     * @param target target id
     */
    VizEdge(std::string id, std::string source, std::string target);

    /**
     * @brief Serialize the edge to the nezviz format.
     * @return serialized edge
     */
    std::string serialize() const;

    std::string id;
    std::string source;
    std::string target;
};

/**
 * @brief Data model to visualize nodes in the nezviz format.
 */
class VizNode {
  public:
    /**
     * @brief Creates a new viz node object, which represents a edge in the nezviz format
     * @param id edge id
     * @param label node label
     * @param parent node parent
     */
    VizNode(std::string id, std::string label, std::string parent);

    /**
     * @brief Creates a new viz node object, which represents a edge in the nezviz format
     * @param id edge id
     * @param label node label
     */
    VizNode(std::string id, std::string label);

    /**
     * @brief Add properties to the node
     */
    void addProperty(const std::tuple<std::string, std::string>&);

    /**
     * @brief Serialize the edge to the nezviz format.
     * @return serialized node
     */
    std::string serialize();
    std::string id;
    std::string label;
    std::string parent;
    std::vector<std::tuple<std::string, std::string>> properties;
};

/**
 * @brief Data model to visualize query graphs in the nezviz format.
 */
class VizGraph {
  public:
    /**
     * @brief Creates a new viz graph object
     * @param name of the graph
     */
    explicit VizGraph(std::string name);

    /**
     * @brief Serialize the edge to the nezviz format.
     * @return serialized graph
     */
    std::string serialize();

    std::string name;
    std::vector<VizEdge> edges;
    std::vector<VizNode> nodes;
};
}// namespace detail

/**
 * @brief Dump handler to dump query plans to a viznez file.
 */
class VizDumpHandler : public DumpHandler {

  public:
    virtual ~VizDumpHandler() = default;
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
    explicit VizDumpHandler(std::string rootDir);

    void dump(NodePtr node) override;

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
    void dump(std::string scope, std::string name, QueryCompilation::PipelineQueryPlanPtr pipelinePlan) override;

  private:
    static void extractNodeProperties(detail::VizNode& node, const OperatorNodePtr& operatorNode);
    void dump(QueryPlanPtr queryPlan, const std::string& parent, detail::VizGraph& graph);
    void writeToFile(const std::string& scope, const std::string& name, const std::string& content);
    std::string rootDir;
};

}// namespace NES

#endif  // NES_INCLUDE_NODES_UTIL_VIZDUMPHANDLER_HPP_
