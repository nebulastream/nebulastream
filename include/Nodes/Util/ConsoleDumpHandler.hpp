#ifndef NES_INCLUDE_NODES_UTIL_CONSOLE_DUMP_HANDLER_HPP_
#define NES_INCLUDE_NODES_UTIL_CONSOLE_DUMP_HANDLER_HPP_

#include <Nodes/Util/DumpHandler.hpp>
#include <memory>
namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

/**
 * @brief Convert a compiler node to a human readable string and print it to the console.
 */
class ConsoleDumpHandler : public DumpHandler {

  public:
    static DebugDumpHandlerPtr create();
    ConsoleDumpHandler();
    /**
    * Dump the specific node and its children.
    */
    void dump(const NodePtr node, std::ostream& out) override;

    /**
    * Dump the specific node and its children with details in multiple lines.
    */
    void multilineDump(const NodePtr node, std::ostream& out);

  private:
    void dumpHelper(const NodePtr op, size_t depth, size_t indent, std::ostream& out) const;
    void multilineDumpHelper(const NodePtr op, size_t depth, size_t indent, std::ostream& out) const;
};

}// namespace NES

#endif//NES_INCLUDE_NODES_UTIL_CONSOLE_DUMP_HANDLER_HPP_
