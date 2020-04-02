#include <Nodes/Util/DumpHandler.hpp>
#include <Nodes/Util/DumpContext.hpp>

namespace NES {

DumpContextPtr DumpContext::create() {
    return std::make_shared<DumpContext>();
}

void DumpContext::registerDumpHandler(DebugDumpHandlerPtr debugDumpHandler) {
    dumpHandlers.push_back(debugDumpHandler);
}

void DumpContext::dump(const NodePtr node, std::ostream& out) {
    for (auto& handler:dumpHandlers) {
        handler->dump(node, out);
    }
}
}