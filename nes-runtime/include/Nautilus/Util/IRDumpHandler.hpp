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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_IRDUMPHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_IRDUMPHANDLER_HPP_

#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <memory>
#include <unordered_set>

namespace NES::Nautilus::IR {

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
    * @brief Dump the NESIR of the funcOp into the 'out' stringstream.
    * @param funcOp: FunctionOperation that exists on the top level of a NESIR module.
    */
    void dump(const std::shared_ptr<Operations::FunctionOperation> funcOp);

  private:
    std::ostream& out;
    std::unordered_set<std::string> visitedBlocks;//We keep track of visited blocks to avoid multi or infinite dumping.

    /**
     * @brief Traverses the NESIR to find a BB that is on the same or higher 'blockScopeLevel' compared to the initial 'basicBlock'.
     *        Note: There is always a 'next block', since we always have a return block at the very end of a function.
     * @param basicBlock: Initially the block that we want to find the next BB for. Replaced while recursively traversing NESIR. 
     * @param blockScopeLevel: The scopeLevel of the initial BB that we are searching the next same/higher level BB for.
     * @return IR::BasicBlockPtr: SharedPtr to the next block that resides on the same or on a higher level.
     */
    IR::BasicBlockPtr getNextLowerOrEqualLevelBasicBlock(IR::BasicBlockPtr basicBlock, int blockScopeLevel);

    /**
     * @brief Handle dumping terminator operations(LoopOp, BranchOp, IfOp, ReturnOp) to the 'out' stringstream.
     * 
     * @param terminatorOp: Terminator operation that we append to the 'out' stringstream.
     * @param scopeLevel: scopeLevel of the BasicBlock that is terminated by the terminator operation.
     */
    void dumpHelper(OperationPtr const& terminatorOp, int32_t scopeLevel);

    /**
     * @brief Handle dumping BasicBlocks to the 'out' stringstream. Print all operations, then handle the terminatorOp.
     * 
     * @param basicBlock: The basicBlock that is dumped to the 'out' stringstream.
     */
    void dumpHelper(BasicBlockPtr const& basicBlock);
};

}// namespace NES::Nautilus::IR

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_IRDUMPHANDLER_HPP_
