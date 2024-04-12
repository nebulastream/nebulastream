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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_IFOPERATION_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_IFOPERATION_HPP_

#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopInfo.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <memory>
#include <string>

namespace NES::Nautilus::IR::Operations {
class IfOperation : public Operation {
  public:
    explicit IfOperation(const Operation& booleanValue);
    ~IfOperation() override = default;

    [[nodiscard]] const Operation& getValue() const;
    BasicBlockInvocation& getTrueBlockInvocation();

    const BasicBlock* getMergeBlock() const;
    const Operation& getBooleanValue() const;
    void setMergeBlock(const BasicBlock& mergeBlock);
    const BasicBlockInvocation& getTrueBlockInvocation() const;
    BasicBlockInvocation& getFalseBlockInvocation();
    const BasicBlockInvocation& getFalseBlockInvocation() const;
    void setTrueBlockInvocation(BasicBlock& trueBlockInvocation);
    void setFalseBlockInvocation(BasicBlock& falseBlockInvocation);
    bool hasFalseCase() const;

    [[nodiscard]] [[nodiscard]] std::string toString() const override;

  private:
    const Operation& booleanValue;
    BasicBlockInvocation trueBlockInvocation;
    BasicBlockInvocation falseBlockInvocation;
    const BasicBlock* mergeBlock = nullptr;
    std::unique_ptr<CountedLoopInfo> countedLoopInfo;
};
}// namespace NES::Nautilus::IR::Operations
#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_IFOPERATION_HPP_
