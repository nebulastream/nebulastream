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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_FLOUNDER_FLOUNDERLOWERINGPROVIDER_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_FLOUNDER_FLOUNDERLOWERINGPROVIDER_HPP_

#include <Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/CastOperation.hpp>
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
#include <Nautilus/Util/Frame.hpp>
#include <Util/DumpHelper.hpp>
#include <flounder/compilation/compiler.h>
#include <flounder/ir/instructions.h>
#include <flounder/ir/register.h>
#include <set>

namespace flounder {
class Executable;
}
namespace NES::Nautilus::Backends::Flounder {

class FlounderLoweringProvider {
  public:
    FlounderLoweringProvider();
    std::unique_ptr<flounder::Executable> lower(std::shared_ptr<IR::IRGraph> ir, const NES::DumpHelper& helper);

  private:
    using FlounderFrame = Frame<std::string, flounder::Register>;
    class LoweringContext {
      public:
        LoweringContext(std::shared_ptr<IR::IRGraph> ir);
        std::unique_ptr<flounder::Executable> process(flounder::Compiler& compiler, const NES::DumpHelper&);
        void process(const std::shared_ptr<IR::Operations::FunctionOperation>&);
        void process(const std::shared_ptr<IR::BasicBlock>&, FlounderFrame& frame);
        void processInline(const std::shared_ptr<IR::BasicBlock>&, FlounderFrame& frame);
        void process(const std::shared_ptr<IR::Operations::Operation>&, FlounderFrame& frame);
        FlounderFrame processBlockInvocation(IR::Operations::BasicBlockInvocation&, FlounderFrame& frame);
        FlounderFrame processInlineBlockInvocation(IR::Operations::BasicBlockInvocation&, FlounderFrame& frame);
        flounder::Register createVreg(IR::Operations::OperationIdentifier id, IR::Types::StampPtr stamp, FlounderFrame& frame);
        flounder::VregInstruction requestVreg(flounder::Register& reg, const IR::Types::StampPtr& stamp);

      private:
        flounder::Program program;
        std::shared_ptr<IR::IRGraph> ir;
        std::set<std::string> activeBlocks;
        void process(const std::shared_ptr<IR::Operations::AddOperation>& opt, FlounderFrame& frame);
        void process(const std::shared_ptr<IR::Operations::CastOperation>& opt, FlounderFrame& frame);
        void process(const std::shared_ptr<IR::Operations::MulOperation>& mulOp, FlounderFrame& frame);
        void process(const std::shared_ptr<IR::Operations::SubOperation>& subOp, FlounderFrame& frame);
        void process(std::shared_ptr<IR::Operations::IfOperation> opt, FlounderFrame& frame);
        void process(const std::shared_ptr<IR::Operations::BranchOperation>& opt, FlounderFrame& frame);
        void process(const std::shared_ptr<IR::Operations::LoopOperation>& opt, FlounderFrame& frame);
        void process(const std::shared_ptr<IR::Operations::LoadOperation>& opt, FlounderFrame& frame);
        void process(const std::shared_ptr<IR::Operations::StoreOperation>& opt, FlounderFrame& frame);
        void process(const std::shared_ptr<IR::Operations::ProxyCallOperation>& opt, FlounderFrame& frame);
        void processAnd(const std::shared_ptr<IR::Operations::AndOperation>& opt,
                        FlounderFrame& frame,
                        flounder::Label& trueCase,
                        flounder::Label& falseCase);
        void processOr(const std::shared_ptr<IR::Operations::OrOperation>& opt,
                       FlounderFrame& frame,
                       flounder::Label& trueCase,
                       flounder::Label& falseCase);
        void processCmp(const std::shared_ptr<IR::Operations::Operation>& opt,
                        FlounderFrame& frame,
                        flounder::Label& trueCase,
                        flounder::Label& falseCase);
        void processCmp(const std::shared_ptr<IR::Operations::CompareOperation>& opt,
                        FlounderFrame& frame,
                        flounder::Label& falseCase);
    };
};

}// namespace NES::Nautilus::Backends::Flounder

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_FLOUNDER_FLOUNDERLOWERINGPROVIDER_HPP_
