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
#include <Experimental/Interpreter/DataValue/Integer.hpp>
#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <Experimental/NESIR/NESIR.hpp>
#include <Experimental/NESIR/Operations/AddIntOperation.hpp>
#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/ConstantIntOperation.hpp>
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
    class IRConversionContext {
      public:
        IRConversionContext(std::shared_ptr<ExecutionTrace> trace) : trace(trace), ir(std::make_shared<IR::NESIR>()){};
        std::shared_ptr<IR::NESIR> process();

      private:
        IR::BasicBlockPtr processBlock(int32_t scope, Block& block);
        void processOperation(int32_t scope, IR::BasicBlockPtr& currentBlock, Operation& operation);
        void processJMP(int32_t scope, IR::BasicBlockPtr& block, Operation& operation);
        void processCMP(int32_t scope, IR::BasicBlockPtr& currentBlock, Operation& operation);
        std::vector<std::string> createBlockArguments(BlockRef val);
        std::string createValueIdentifier(InputVariant val);
      private:
        std::shared_ptr<ExecutionTrace> trace;
        std::shared_ptr<IR::NESIR> ir;
        std::unordered_map<uint32_t, IR::BasicBlockPtr> blockMap;
    };
};

}// namespace NES::ExecutionEngine::Experimental::Trace

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_TRACE_TRACETOIRCONVERSIONPHASE_HPP_
