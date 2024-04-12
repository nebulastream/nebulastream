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

#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/Util/IRDumpHandler.hpp>
#include <sstream>
#include <utility>

namespace NES::Nautilus::IR {

IRGraph::IRGraph(Flags flags) : flags(std::move(flags)) {}

Operations::FunctionOperation& IRGraph::addRootOperation(std::unique_ptr<Operations::FunctionOperation>&& rootOperation) {
    this->rootOperation = std::move(rootOperation);
    return *this->rootOperation;
}

Operations::FunctionOperation& IRGraph::getRootOperation() { return *rootOperation; }

std::string IRGraph::toString() {
    std::stringstream ss;
    ss << "NESIR {\n";
    auto dumpHandler = Nautilus::IR::NESIRDumpHandler::create(ss);
    dumpHandler->dump(*rootOperation);
    ss << "} //NESIR";
    return ss.str();
}
const IRGraph::Flags& IRGraph::getFlags() const { return flags; }
void IRGraph::setFlags(const IRGraph::Flags& flags) { IRGraph::flags = flags; }

const std::string& IRGraph::Flags::getDumpOutputPath() const { return dumpOutputPath; }

IRGraph::Flags::Flags() = default;
void IRGraph::Flags::setDumpOutputPath(const std::string& dumpOutputPath) { Flags::dumpOutputPath = dumpOutputPath; }
bool IRGraph::Flags::isDumpToFile() const { return dumpToFile; }
void IRGraph::Flags::setDumpToFile(bool dumpToFile) { Flags::dumpToFile = dumpToFile; }
bool IRGraph::Flags::isDumpToConsole() const { return dumpToConsole; }
void IRGraph::Flags::setDumpToConsole(bool dumpToConsole) { Flags::dumpToConsole = dumpToConsole; }
bool IRGraph::Flags::isOptimize() const { return optimize; }
void IRGraph::Flags::setOptimize(bool optimize) { Flags::optimize = optimize; }
bool IRGraph::Flags::isDebug() const { return debug; }
void IRGraph::Flags::setDebug(bool debug) { Flags::debug = debug; }
}// namespace NES::Nautilus::IR
