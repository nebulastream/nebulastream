
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Node.hpp>
#include <iostream>
namespace NES {

ConsoleDumpHandler::ConsoleDumpHandler(): DumpHandler(){}

DebugDumpHandlerPtr ConsoleDumpHandler::create() {
    return std::make_shared<ConsoleDumpHandler>();
}

void ConsoleDumpHandler::dumpHelper(const NodePtr op, size_t depth, size_t indent, std::ostream& out) const {
    out << std::string(indent*depth, ' ') << op->toString() << std::endl;
    ++depth;
    auto children = op->getChildren();
    for (auto&& child: children) {
        dumpHelper(child, depth, indent, out);
    }
}

void ConsoleDumpHandler::dump(const NodePtr node, std::ostream& out) {
    dumpHelper(node, /*depth*/0, /*indent*/2, out);
}
}
