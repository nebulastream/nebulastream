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

};

typedef std::shared_ptr<DumpHandler> DebugDumpHandlerPtr;

}

#endif //NES_INCLUDE_NODES_UTIL_DUMPHANDLER_HPP_
