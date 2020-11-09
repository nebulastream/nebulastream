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

#ifndef NES_INCLUDE_NODES_UTIL_DUMPCONTEXT_HPP_
#define NES_INCLUDE_NODES_UTIL_DUMPCONTEXT_HPP_

#include <memory>
#include <vector>
namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

class DumpHandler;
typedef std::shared_ptr<DumpHandler> DebugDumpHandlerPtr;

class DumpContext;
typedef std::shared_ptr<DumpContext> DumpContextPtr;

/**
 * @brief The dump context is used to dump a node graph to multiple dump handler at the same time.
 * To this end, it manages the local registered dump handlers.
 */
class DumpContext {
  public:
    static DumpContextPtr create();

    /**
     * @brief Registers a new dump handler to the context.
     * @param debugDumpHandler
     */
    void registerDumpHandler(DebugDumpHandlerPtr debugDumpHandler);
    /**
     * @brief Dumps the passed node and its children on all registered dump handlers.
     * @param node
     */
    void dump(const NodePtr node, std::ostream& out);

  private:
    std::vector<DebugDumpHandlerPtr> dumpHandlers;
};

}// namespace NES

#endif//NES_INCLUDE_NODES_UTIL_DUMPCONTEXT_HPP_
