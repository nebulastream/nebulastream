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

void ConsoleDumpHandler::multilineDumpHelper(const NodePtr op, size_t depth, size_t indent, std::ostream& out) const {

    std::vector<std::string> multiLineNodeString = op->toMultilineString();
    for (const std::string line : multiLineNodeString) {
        for (int i = 0; i < indent * depth; i++) {
            if (i % indent == 0) {
                out << '|';
            } else {
                if (line == multiLineNodeString.front() && i >= indent * depth - 1) {
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
        multilineDumpHelper(child, depth, indent, out);
    }
}

void ConsoleDumpHandler::dump(const NodePtr node, std::ostream& out) {
    dumpHelper(node, /*depth*/ 0, /*indent*/ 2, out);
}

void ConsoleDumpHandler::multilineDump(const NodePtr node, std::ostream& out) {
    multilineDumpHelper(node, /*depth*/ 0, /*indent*/ 2, out);
}
}// namespace NES
