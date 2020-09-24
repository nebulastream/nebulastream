
#include <Nodes/Node.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <iostream>
namespace NES {

ConsoleDumpHandler::ConsoleDumpHandler() : DumpHandler() {}

DebugDumpHandlerPtr ConsoleDumpHandler::create() {
    return std::make_shared<ConsoleDumpHandler>();
}

void ConsoleDumpHandler::dumpHelper(const NodePtr op, size_t depth, size_t indent, std::ostream& out) const {
    out << std::string(indent * depth, ' ') << op->toString() << std::endl;
    ++depth;
    auto children = op->getChildren();
    for (auto&& child : children) {
        dumpHelper(child, depth, indent, out);
    }
}

void ConsoleDumpHandler::multilineDumpHelper(const NodePtr op, size_t depth, size_t indent, std::ostream& out, bool isLastChild) const {

    std::vector<std::string> multiLineNodeString = op->toMultilineString();
    for (const std::string line: multiLineNodeString) {
        for (int i=0; i<indent * depth; i++) {
            if (i%indent==0) {
                out << '|';
                if (isLastChild) {
//                    out << 'L';
                }
            } else {
                if (line == multiLineNodeString.front() && i >= indent * depth-1) {
                    out << std::string(indent, '-');
                } else {
                    out << std::string(indent, ' ');
                }
            }
        }
        if (line != multiLineNodeString.front()) {
            out << '|' << ' ';
        }
        out << line << std::endl;
    }
    ++depth;
    auto children = op->getChildren();
    for (auto&& child : children) {
        multilineDumpHelper(child, depth, indent, out, child == children.back());
    }
}

void ConsoleDumpHandler::dump(const NodePtr node, std::ostream& out) {
    dumpHelper(node, /*depth*/ 0, /*indent*/ 2, out);
}

void ConsoleDumpHandler::multilineDump(const NodePtr node, std::ostream& out) {
    multilineDumpHelper(node, /*depth*/ 0, /*indent*/ 2, out, false);
}}// namespace NES
