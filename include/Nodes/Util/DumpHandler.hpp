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

#ifndef NES_INCLUDE_NODES_UTIL_DUMPHANDLER_HPP_
#define NES_INCLUDE_NODES_UTIL_DUMPHANDLER_HPP_

#include <memory>
namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

/**
 * @brief Implemented by classes that provide an visualization of passed nodes. The format and client required to consume the visualizations
 * is determined by the implementation. For example, a dumper may convert a compiler node to a human
 * readable string and print it to the console. A more sophisticated dumper may serialize a compiler
 * graph and send it over the network to a tool (e.g., https://github.com/graalvm/visualizer) that
 * can display graphs.
 */
class DumpHandler {
  public:
    DumpHandler() = default;
    /**
    * Dump the specific node and its children.
    */
    virtual void dump(const NodePtr node, std::ostream& out) = 0;
    virtual void multilineDump(const NodePtr node, std::ostream& out) = 0;
};

typedef std::shared_ptr<DumpHandler> DebugDumpHandlerPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_UTIL_DUMPHANDLER_HPP_
