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

#ifndef NES_BASICBLOCK_HPP
#define NES_BASICBLOCK_HPP

#include <Experimental/NESIR/Operations/Operation.hpp>
#include <memory>
#include <vector>

namespace NES::ExecutionEngine::Experimental::IR {

class BasicBlock : public std::enable_shared_from_this<BasicBlock> {
  public:
    /**
     * @brief BasicBlock used for control flow in NES IR
     * @param Operations: A list of Operations that are executed in the BasicBlock.
     * @param nextBlocks : The BasicBlock that is next in the control flow of the execution.
     */
    explicit BasicBlock(std::string identifier,
                        int32_t scopeLevel,
                        std::vector<Operations::OperationPtr> operations,
                        std::vector<std::string> inputArgs,
                        std::vector<Operations::PrimitiveStamp> inputArgTypes);
    virtual ~BasicBlock() = default;
    [[nodiscard]] std::string getIdentifier();
    [[nodiscard]] int32_t getScopeLevel();
    [[nodiscard]] std::vector<Operations::OperationPtr> getOperations();
    [[nodiscard]] Operations::OperationPtr getTerminatorOp();
    [[nodiscard]] std::vector<std::string> getInputArgs();
    [[nodiscard]] std::vector<Operations::PrimitiveStamp> getInputArgTypes();

    // NESIR Assembly
    std::shared_ptr<BasicBlock> addOperation(Operations::OperationPtr operation);
    std::shared_ptr<BasicBlock> addLoopHeadBlock(std::shared_ptr<BasicBlock> loopHeadBlock);
    std::shared_ptr<BasicBlock> addNextBlock(std::shared_ptr<BasicBlock> nextBlock);
    std::shared_ptr<BasicBlock> addThenBlock(std::shared_ptr<BasicBlock> thenBlock);
    std::shared_ptr<BasicBlock> addElseBlock(std::shared_ptr<BasicBlock> elseBlock);

    void popOperation();

  private:
    std::string identifier;
    int32_t scopeLevel;
    std::vector<Operations::OperationPtr> operations;
    std::vector<std::string> inputArgs;
    std::vector<Operations::PrimitiveStamp> inputArgTypes;
};
using BasicBlockPtr = std::shared_ptr<BasicBlock>;

}// namespace NES::ExecutionEngine::Experimental::IR
#endif//NES_BASICBLOCK_HPP
