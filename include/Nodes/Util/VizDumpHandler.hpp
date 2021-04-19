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

class VizEdge{
  public:
    VizEdge(std::string id, std::string source, std::string target);
    std::string id;
    std::string source;
    std::string target;
    std::string serialize();
};

class VizNode{
  public:
    VizNode(std::string id, std::string label, std::string parent);
    VizNode(std::string id, std::string label);
    std::string id;
    std::string label;
    std::string parent;
    std::string serialize();
};

class VizGraph{
  public:
    VizGraph(std::string name);
    std::string name;
    std::vector<VizEdge> edges;
    std::vector<VizNode> nodes;
    std::string serialize();
};

/**
 * @brief Convert a compiler node to a human readable string and print it to the console.
 */
class VizDumpHandler : public DumpHandler {

  public:
    static DebugDumpHandlerPtr create();
    VizDumpHandler(std::string rootDir);
    /**
    * Dump the specific node and its children.
    */
    void dump(const NodePtr node, std::ostream& out) override;
    virtual void multilineDump(const NodePtr node, std::ostream& out);
    void dump(std::string context, std::string scope, QueryPlanPtr queryPlan) override;
    void dump(QueryPlanPtr queryPlan, std::string parent, VizGraph& graph);
    void dump(std::string scope, std::string name, QueryCompilation::PipelineQueryPlanPtr ptr) override;
    void writeToFile(std::string scope, std::string name, std::string content);

  private:
    std::string rootDir;
};

}// namespace NES

#endif//NES_INCLUDE_NODES_UTIL_VIZ_DUMP_HANDLER_HPP_
