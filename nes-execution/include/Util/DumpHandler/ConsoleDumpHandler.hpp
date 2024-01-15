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

#ifndef NES_EXECUTION_INCLUDE_UTIL_DUMPHANDLER_CONSOLEDUMPHANDLER_HPP_
#define NES_EXECUTION_INCLUDE_UTIL_DUMPHANDLER_CONSOLEDUMPHANDLER_HPP_

#include <memory>

namespace NES {

namespace QueryCompilation {
class PipelineQueryPlan;
using PipelineQueryPlanPtr = std::shared_ptr<PipelineQueryPlan>;
}// namespace QueryCompilation

class Node;
using NodePtr = std::shared_ptr<Node>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

/**
 * @brief Converts query plans and pipeline plans to the .nesviz format and dumps them to a file.m
 */
class ConsoleDumpHandler {

  public:
    virtual ~ConsoleDumpHandler() = default;
    static std::shared_ptr<ConsoleDumpHandler> create(std::ostream& out);
    explicit ConsoleDumpHandler(std::ostream& out);
    /**
    * Dump the specific node and its children.
    */
    void dump(NodePtr node);

    /**
    * Dump the specific node and its children with details in multiple lines.
    */
    void multilineDump(NodePtr const& node);

    /**
     * @brief Dump a pipeline query plan with a specific context and scope.
     * @param context the context
     * @param scope the scope
     * @param decomposedQueryPlan the decomposed query plan
     */
    void dump(std::string context, std::string scope, DecomposedQueryPlanPtr decomposedQueryPlan);

    /**
     * @brief Dump a pipeline query plan with a specific context and scope.
     * @param context the context
     * @param scope the scope
     * @param plan the query plan
     */
    void dump(std::string context, std::string scope, QueryCompilation::PipelineQueryPlanPtr pipelineQueryPlan);

  private:
    std::ostream& out;
    void dumpHelper(NodePtr const& op, uint64_t depth, uint64_t indent, std::ostream& out) const;
    void multilineDumpHelper(NodePtr const& op, uint64_t depth, uint64_t indent, std::ostream& out) const;
};

}// namespace NES

#endif// NES_EXECUTION_INCLUDE_UTIL_DUMPHANDLER_CONSOLEDUMPHANDLER_HPP_
