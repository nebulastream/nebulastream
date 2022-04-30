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

#ifndef NES_LOOPBASICBLOCK_HPP
#define NES_LOOPBASICBLOCK_HPP

#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>

namespace NES {
/**
 * @brief Iterates over a buffer. Loads a record on each iteration. Contains operations which are nested inside of the LoopBlock.
 * Points to other BasicBlocks if there is control flow.
 */
class LoopBasicBlock : public BasicBlock {
  public:
    LoopBasicBlock(std::vector<OperationPtr> operators, BasicBlockPtr nextBlock, void* inputBufferPtr, uint64_t upperLimit);
    ~LoopBasicBlock() override = default;

    [[nodiscard]] uint64_t getUpperLimit() const;
    void* getInputBufferPtr();
    static bool classof(const BasicBlock *Block);

  private:
    BasicBlockPtr nextBlock;
    void* inputBufferPtr; // Cannot include TupleBuffer currently
    uint64_t upperLimit;



};
}// namespace NES
#endif//NES_LOOPBASICBLOCK_HPP
