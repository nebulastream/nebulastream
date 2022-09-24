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
#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_TRACE_TRACETOIRCONVERSIONPHASE_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_TRACE_TRACETOIRCONVERSIONPHASE_HPP_
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Experimental/NESIR/Frame.hpp>
#include <Nautilus/IR/NESIR.hpp>
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
namespace NES::Nautilus::Tracing {

class TraceToIRConversionPhase {
  public:
    std::shared_ptr<ExecutionEngine::Experimental::IR::NESIR> apply(std::shared_ptr<ExecutionTrace> trace);

  private:
    using ValueFrame = ExecutionEngine::Experimental::IR::Frame<std::string, ExecutionEngine::Experimental::IR::Operations::OperationPtr>;
    class IRConversionContext {
      public:
        IRConversionContext(std::shared_ptr<ExecutionTrace> trace) : trace(trace), ir(std::make_shared<ExecutionEngine::Experimental::IR::NESIR>()){};
        std::shared_ptr<ExecutionEngine::Experimental::IR::NESIR> process();

      private:
        ExecutionEngine::Experimental::IR::BasicBlockPtr processBlock(int32_t scope, Block& block);
        void processOperation(int32_t scope,
                              ValueFrame& frame,
                              Block& currentBlock,
                              ExecutionEngine::Experimental::IR::BasicBlockPtr& currentIRBlock,
                              TraceOperation& operation);
        void processJMP(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& block, TraceOperation& operation);
        void processCMP(int32_t scope,
                        ValueFrame& frame,
                        Block& currentBlock,
                        ExecutionEngine::Experimental::IR::BasicBlockPtr& currentIRBlock,
                        TraceOperation& operation);
        void processAdd(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processSub(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processMul(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processDiv(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processEquals(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processLessThan(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processGreaterThan(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processNegate(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processAnd(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processOr(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processLoad(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processStore(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processCall(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processConst(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        void processCast(int32_t scope, ValueFrame& frame, ExecutionEngine::Experimental::IR::BasicBlockPtr& currentBlock, TraceOperation& operation);
        bool isBlockInLoop(uint32_t parentBlock, uint32_t currentBlock);
        std::vector<std::string> createBlockArguments(BlockRef val);
        void createBlockArguments(ValueFrame& frame, ExecutionEngine::Experimental::IR::Operations::BasicBlockInvocation& blockInvocation, BlockRef val);
        std::string createValueIdentifier(InputVariant val);

      private:
        std::shared_ptr<ExecutionTrace> trace;
        std::shared_ptr<ExecutionEngine::Experimental::IR::NESIR> ir;
        bool isSCF = true;
        std::unordered_map<uint32_t, ExecutionEngine::Experimental::IR::BasicBlockPtr> blockMap;
        ExecutionEngine::Experimental::IR::BasicBlockPtr findControlFlowMerge(ExecutionEngine::Experimental::IR::BasicBlockPtr currentBlock, int32_t targetScope);
    };
};

}// namespace NES::Nautilus::Tracing

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_TRACE_TRACETOIRCONVERSIONPHASE_HPP_
