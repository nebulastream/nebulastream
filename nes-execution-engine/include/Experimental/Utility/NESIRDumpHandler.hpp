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

#ifndef NES_INCLUDE_NODES_UTIL_NESIRDUMPHANDLER_HPP_
#define NES_INCLUDE_NODES_UTIL_NESIRDUMPHANDLER_HPP_

#include "Experimental/NESIR/BasicBlocks/BasicBlock.hpp"
#include "Experimental/NESIR/NESIR.hpp"
// #include <Experimental/NESIR/Operations/Operation.hpp>
#include <memory>

namespace NES {
namespace ExecutionEngine::Experimental::IR {

// class Operation;
using OperationPtr = std::shared_ptr<Operations::Operation>;
/**
 * @brief Converts query plans and pipeline plans to the .nesviz format and dumps them to a file.m
 */
class NESIRDumpHandler {

  public:
    virtual ~NESIRDumpHandler() = default;
    static std::shared_ptr<NESIRDumpHandler> create(std::ostream& out);
    explicit NESIRDumpHandler(std::ostream& out);
    /**
    * Dump the specific node and its children.
    */
    void dump(const std::shared_ptr<Operations::FunctionOperation> funcOp);

  private:
    std::ostream& out;
    void dumpHelper(OperationPtr const& op, uint64_t indent, std::ostream& out) const;
    void dumpHelper(BasicBlockPtr const& basicBlock, uint64_t indent, std::ostream& out) const;
};

}// namespace ExecutionEngine::Experimental::IR
}// namespace NES

#endif// NES_INCLUDE_NODES_UTIL_NESIRDUMPHANDLER_HPP_
