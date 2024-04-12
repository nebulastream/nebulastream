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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_LOOP_LOOPOPERATION_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_LOOP_LOOPOPERATION_HPP_

#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopInfo.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <vector>

namespace NES::Nautilus::IR::Operations {
/**
 * @brief Iterates over a buffer. Loads a record on each iteration. Contains operations which are nested inside of the LoopBlock.
 * Points to other BasicBlocks if there is control flow.
 */
class LoopOperation : public Operation {
  public:
    enum class LoopType : uint8_t { DefaultLoop, CountedLoop };
    LoopOperation(LoopType loopType);
    ~LoopOperation() override = default;

    LoopType getLoopType();
    [[nodiscard]] const BasicBlockInvocation& getLoopBodyBlock() const;
    BasicBlockInvocation& getLoopFalseBlock();
    [[nodiscard]] const BasicBlockInvocation& getLoopFalseBlock() const;
    BasicBlockInvocation& getLoopHeadBlock();
    const BasicBlockInvocation& getLoopHeadBlock() const;
    BasicBlockInvocation& getLoopEndBlock();
    const BasicBlockInvocation& getLoopEndBlock() const;
    void setLoopInfo(std::shared_ptr<LoopInfo> loopInfo);
    std::shared_ptr<LoopInfo> getLoopInfo();
    void setLoopType(LoopOperation::LoopType loopType);
    BasicBlockInvocation& getLoopBodyBlock();

    [[nodiscard]] std::string toString() const override;

  private:
    LoopType loopType;
    BasicBlockInvocation loopHeadBlock;
    BasicBlockInvocation loopBodyBlock;
    BasicBlockInvocation loopFalseBlock;
    BasicBlockInvocation loopEndBlock;
    std::shared_ptr<LoopInfo> loopInfo;
};
}// namespace NES::Nautilus::IR::Operations
#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_LOOP_LOOPOPERATION_HPP_
