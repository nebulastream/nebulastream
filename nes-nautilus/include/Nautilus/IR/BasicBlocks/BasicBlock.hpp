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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_IR_BASICBLOCKS_BASICBLOCK_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_IR_BASICBLOCKS_BASICBLOCK_HPP_

#include <Nautilus/IR/BasicBlocks/BasicBlockArgument.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <memory>
#include <vector>

namespace NES::Nautilus::IR {

class BasicBlock {
  public:
    /**
     * @brief BasicBlock used for control flow in NES IR
     * @param Operations: A list of Operations that are executed in the BasicBlock.
     * @param nextBlocks : The BasicBlock that is next in the control flow of the execution.
     */
    explicit BasicBlock(std::string identifier,
                        int32_t scopeLevel,
                        std::vector<Operations::OperationPtr> operations,
                        std::vector<std::unique_ptr<Operations::BasicBlockArgument>> arguments);
    virtual ~BasicBlock() = default;
    [[nodiscard]] std::string getIdentifier() const;
    void setIdentifier(const std::string& identifier);
    [[nodiscard]] uint32_t getScopeLevel() const;
    void setScopeLevel(uint32_t scopeLevel);

    /**
     * @brief Get the number of edges that lead back from the loop body to the loop header.
     */
    [[nodiscard]] uint32_t getNumLoopBackEdges() const;

    /**
     * @brief Increment counter for edges that lead back from the loop body to the loop header.
     */
    void incrementNumLoopBackEdge();

    /**
     * @brief Check if the counter for edges that lead back from the loop body to the loop header is > 0.
     */
    [[nodiscard]] bool isLoopHeaderBlock() const;
    [[nodiscard]] std::vector<Operations::OperationRef> getOperations() const;
    [[nodiscard]] Operations::Operation& getOperationAt(size_t index) const;
    [[nodiscard]] Operations::Operation& getTerminatorOp() const;
    [[nodiscard]] std::vector<Operations::OperationRef> getArguments() const;

    // NESIR Assembly
    BasicBlock& addOperation(Operations::OperationPtr&& operation);
    BasicBlock& addLoopHeadBlock(BasicBlock& loopHeadBlock);
    BasicBlock& addNextBlock(BasicBlock& nextBlock);
    void addNextBlock(BasicBlock& nextBlock, const std::vector<Operations::OperationRef>& ops);
    BasicBlock& addTrueBlock(BasicBlock& thenBlock);
    BasicBlock& addFalseBlock(BasicBlock& elseBlock);
    [[nodiscard]] Operations::OperationPtr removeOperation(Operations::Operation& operation);
    void addOperationBefore(const Operations::Operation& before, Operations::OperationPtr&& operation);
    void addPredecessor(BasicBlock& predecessor);
    const std::vector<gsl::not_null<BasicBlock*>>& getPredecessors() const;
    std::vector<gsl::not_null<BasicBlock*>>& getPredecessors();
    [[nodiscard]] ssize_t getIndexOfArgument(const Operations::BasicBlockArgument& arg) const;
    // void popOperation();
    void replaceTerminatorOperation(Operations::OperationPtr newTerminatorOperation);
    [[nodiscard]] std::pair<BasicBlock*, BasicBlock*> getNextBlocks() const;

  private:
    std::string identifier;
    uint32_t scopeLevel;
    uint32_t numLoopBackEdges;
    std::vector<Operations::OperationPtr> operations;
    std::vector<std::unique_ptr<Operations::BasicBlockArgument>> arguments;
    std::vector<gsl::not_null<BasicBlock*>> predecessors;
};

using OwningBasicBlockPtr = std::unique_ptr<BasicBlock>;
using BasicBlockPtr = gsl::not_null<BasicBlock*>;

}// namespace NES::Nautilus::IR
#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_IR_BASICBLOCKS_BASICBLOCK_HPP_
