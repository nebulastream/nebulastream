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
#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <Experimental/NESIR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Experimental/NESIR/Frame.hpp>
#include <Experimental/NESIR/NESIR.hpp>
#include <Experimental/NESIR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/ConstFloatOperation.hpp>
#include <Experimental/NESIR/Operations/ConstIntOperation.hpp>
#include <Experimental/NESIR/Operations/FunctionOperation.hpp>
#include <Experimental/NESIR/Operations/IfOperation.hpp>
#include <Experimental/NESIR/Operations/Operation.hpp>
#include <Experimental/NESIR/Operations/ReturnOperation.hpp>
#include <Experimental/Trace/BlockRef.hpp>
#include <Experimental/Trace/ExecutionTrace.hpp>
#include <Experimental/Trace/OperationRef.hpp>
namespace NES::ExecutionEngine::Experimental::Trace {

class TraceToIRConversionPhase {
  public:
    std::shared_ptr<IR::NESIR> apply(std::shared_ptr<ExecutionTrace> trace);

  private:
    using ValueFrame = IR::Frame<std::string, IR::Operations::OperationPtr>;
    class IRConversionContext {
      public:
        IRConversionContext(std::shared_ptr<ExecutionTrace> trace) : trace(trace), ir(std::make_shared<IR::NESIR>()){};
        std::shared_ptr<IR::NESIR> process();

      private:
        IR::BasicBlockPtr processBlock(int32_t scope, Block& block);
        void processOperation(int32_t scope,
                              ValueFrame& frame,
                              Block& currentBlock,
                              IR::BasicBlockPtr& currentIRBlock,
                              Operation& operation);
        void processJMP(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& block, Operation& operation);
        void processCMP(int32_t scope,
                        ValueFrame& frame,
                        Block& currentBlock,
                        IR::BasicBlockPtr& currentIRBlock,
                        Operation& operation);
        void processAdd(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, Operation& operation);
        void processSub(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, Operation& operation);
        void processMul(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, Operation& operation);
        void processDiv(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, Operation& operation);
        void processEquals(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, Operation& operation);
        void processLessThan(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, Operation& operation);
        void processNegate(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, Operation& operation);
        void processAnd(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, Operation& operation);
        void processOr(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, Operation& operation);
        void processLoad(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, Operation& operation);
        void processStore(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, Operation& operation);
        void processCall(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, Operation& operation);
        void processConst(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, Operation& operation);
        void processCast(int32_t scope, ValueFrame& frame, IR::BasicBlockPtr& currentBlock, Operation& operation);
        bool isBlockInLoop(uint32_t parentBlock, uint32_t currentBlock);
        std::vector<std::string> createBlockArguments(BlockRef val);
        void createBlockArguments(ValueFrame& frame, IR::Operations::BasicBlockInvocation& blockInvocation, BlockRef val);
        std::string createValueIdentifier(InputVariant val);

      private:
        std::shared_ptr<ExecutionTrace> trace;
        std::shared_ptr<IR::NESIR> ir;
        bool isSCF = true;
        std::unordered_map<uint32_t, IR::BasicBlockPtr> blockMap;
        IR::BasicBlockPtr findControlFlowMerge(IR::BasicBlockPtr currentBlock, int32_t targetScope);
    };
};

}// namespace NES::ExecutionEngine::Experimental::Trace

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_TRACE_TRACETOIRCONVERSIONPHASE_HPP_
