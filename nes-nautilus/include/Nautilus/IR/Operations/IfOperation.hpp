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
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopInfo.hpp>

namespace NES::Nautilus::IR::Operations {
class IfOperation : public Operation {
  public:
    IfOperation(OperationPtr booleanValue);
    ~IfOperation() override = default;

    OperationPtr getValue();

    BasicBlockPtr getMergeBlock();
    OperationPtr getBooleanValue();
    void setMergeBlock(BasicBlockPtr mergeBlock);
    BasicBlockInvocation& getTrueBlockInvocation();
    BasicBlockInvocation& getFalseBlockInvocation();
    void setTrueBlockInvocation(BasicBlockPtr trueBlockInvocation);
    void setFalseBlockInvocation(BasicBlockPtr falseBlockInvocation);
    bool hasFalseCase();

    std::string toString() override;

  private:
    OperationWPtr booleanValue;
    BasicBlockInvocation trueBlockInvocation;
    BasicBlockInvocation falseBlockInvocation;
    std::weak_ptr<BasicBlock> mergeBlock;
    std::unique_ptr<CountedLoopInfo> countedLoopInfo;
};
}// namespace NES::Nautilus::IR::Operations
#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_IFOPERATION_HPP_
