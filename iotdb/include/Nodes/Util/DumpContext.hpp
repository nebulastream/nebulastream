#ifndef NES_INCLUDE_NODES_UTIL_DUMPCONTEXT_HPP_
#define NES_INCLUDE_NODES_UTIL_DUMPCONTEXT_HPP_

#include <vector>
#include <memory>
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

}

#endif //NES_INCLUDE_NODES_UTIL_DUMPCONTEXT_HPP_
