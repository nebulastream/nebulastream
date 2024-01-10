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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_PHASES_TRACETOIRCONVERSIONPHASE_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_PHASES_TRACETOIRCONVERSIONPHASE_HPP_
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/ConstFloatOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Operations/ReturnOperation.hpp>
#include <Nautilus/Tracing/Trace/BlockRef.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/Trace/OperationRef.hpp>
#include <Nautilus/Util/Frame.hpp>
#include <memory>
namespace NES::Nautilus::Tracing {

/**
 * @brief Translates a trace into a corresponding IR fragment.
 */
class TraceToIRConversionPhase {
  public:
    /**
     * @brief Performs the conversion and returns a IR fragment for the given trace
     * @param trace
     * @return IR
     */
    std::shared_ptr<IR::IRGraph> apply(std::shared_ptr<ExecutionTrace> trace);

  private:
    using ValueFrame = Frame<std::string, IR::Operations::OperationPtr>;

    /**
     * @brief Internal context object, which maintains statue during IR creation.
     */
    class IRConversionContext {
      public:
        IRConversionContext(std::shared_ptr<ExecutionTrace> trace) : trace(trace), ir(std::make_shared<IR::IRGraph>()){};
        std::shared_ptr<IR::IRGraph> process();

      private:
        IR::BasicBlockPtr processBlock(int32_t scope, Block& block);
        void processOperation(int32_t scope,
                              ValueFrame& frame,
                              Block& currentBlock,
                              IR::BasicBlockPtr& currentIRBlock,
                              TraceOperation& operation);
        void processJMP(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& block, TraceOperation& operation);
        void processCMP(int32_t scope,
                        ValueFrame& frame,
                        Block& currentBlock,
                        IR::BasicBlockPtr& currentIRBlock,
                        TraceOperation& operation);
        void processAdd(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processSub(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processMul(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processDiv(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processMod(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processEquals(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processLessThan(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processGreaterThan(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processNegate(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processAnd(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processOr(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processBitWiseAnd(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processBitWiseOr(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processBitWiseXor(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void
        processBitWiseLeftShift(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void
        processBitWiseRightShift(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processLoad(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processStore(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processCall(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processConst(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processCast(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        bool isBlockInLoop(uint32_t parentBlock, uint32_t currentBlock);
        std::vector<std::string> createBlockArguments(BlockRef val);
        void createBlockArguments(ValueFrame& frame, IR::Operations::BasicBlockInvocation& blockInvocation, BlockRef val);
        std::string createValueIdentifier(InputVariant val);

      private:
        std::shared_ptr<ExecutionTrace> trace;
        std::shared_ptr<IR::IRGraph> ir;
        std::unordered_map<uint32_t, IR::BasicBlockPtr> blockMap;
    };
};

}// namespace NES::Nautilus::Tracing

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_PHASES_TRACETOIRCONVERSIONPHASE_HPP_
