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
#ifndef NES_RUNTIME_INCLUDE_EXPERIMENTAL_FLOUNDER_FLOUNDERLOWERINGPROVIDER_HPP_
#define NES_RUNTIME_INCLUDE_EXPERIMENTAL_FLOUNDER_FLOUNDERLOWERINGPROVIDER_HPP_
#include <Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Nautilus/IR/Frame.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LoadOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/ProxyCallOperation.hpp>
#include <Nautilus/IR/Operations/StoreOperation.hpp>

#include <flounder/compiler.h>
#include <set>

namespace flounder {
class Executable;
}
namespace NES::ExecutionEngine::Experimental::Flounder {

class FlounderLoweringProvider {
  public:
    FlounderLoweringProvider();
    std::unique_ptr<flounder::Executable> lower(std::shared_ptr<IR::NESIR> ir);
    flounder::Compiler compiler = flounder::Compiler{/*do not optimize*/ false,
                                                     /*collect the asm code to print later*/ false,
                                                     /*do not collect asm instruction offsets*/ true};

  private:
    using FlounderFrame = IR::Frame<std::string, flounder::Node*>;
    class LoweringContext {
      public:
        LoweringContext(std::shared_ptr<IR::NESIR> ir);
        std::unique_ptr<flounder::Executable> process(flounder::Compiler& compiler);
        void process(std::shared_ptr<IR::Operations::FunctionOperation>);
        void process(std::shared_ptr<IR::BasicBlock>, FlounderFrame& frame);
        void processInline(std::shared_ptr<IR::BasicBlock>, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::Operation>, FlounderFrame& frame);
        FlounderFrame processBlockInvocation(IR::Operations::BasicBlockInvocation&, FlounderFrame& frame);
        FlounderFrame processInlineBlockInvocation(IR::Operations::BasicBlockInvocation&, FlounderFrame& frame);
        flounder::VirtualRegisterIdentifierNode*
        createVreg(IR::Operations::OperationIdentifier id, IR::Types::StampPtr stamp, FlounderFrame& frame);

      private:
        flounder::Program program;
        std::shared_ptr<IR::NESIR> ir;
        std::set<std::string> activeBlocks;
        void process(std::shared_ptr<IR::Operations::AddOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::MulOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::SubOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::IfOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::CompareOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::BranchOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::LoopOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::LoadOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::StoreOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::ProxyCallOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::OrOperation> opt, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::AndOperation> opt, FlounderFrame& frame);
        void processAnd(std::shared_ptr<IR::Operations::AndOperation> opt, FlounderFrame& frame, flounder::LabelNode* falseCase);
        void
        processCmp(std::shared_ptr<IR::Operations::CompareOperation> opt, FlounderFrame& frame, flounder::LabelNode* falseCase);
    };
};

}// namespace NES::ExecutionEngine::Experimental::Flounder

#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_FLOUNDER_FLOUNDERLOWERINGPROVIDER_HPP_
