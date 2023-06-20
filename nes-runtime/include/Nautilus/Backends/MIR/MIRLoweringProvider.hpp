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
#ifndef NES_RUNTIME_INCLUDE_EXPERIMENTAL_FLOUNDER_MIRLOWERINGPROVIDER_HPP_
#define NES_RUNTIME_INCLUDE_EXPERIMENTAL_FLOUNDER_MIRLOWERINGPROVIDER_HPP_

#include "Nautilus/IR/Operations/Operation.hpp"
#include "Nautilus/IR/Types/Stamp.hpp"
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
#include <map>
#include <mir.h>
#include <set>

typedef uint32_t MIR_reg_t;
namespace NES::Nautilus::Backends::MIR {

class MIRLoweringProvider {
  public:
    MIRLoweringProvider();
    MIR_item_t lower(std::shared_ptr<IR::IRGraph> ir, const NES::DumpHelper& helper, MIR_context_t ctx, MIR_module_t* m);

  private:
    using MIRFrame = Frame<IR::Operations::OperationIdentifier, MIR_reg_t>;
    class LoweringContext {
      public:
        LoweringContext(MIR_context_t ctx, MIR_module_t* m, std::shared_ptr<IR::IRGraph> ir);
        MIR_item_t process(const NES::DumpHelper&);
        void process(const std::shared_ptr<IR::Operations::FunctionOperation>&);
        void processBlock(const std::shared_ptr<IR::BasicBlock>& block, MIRFrame& frame);
        void processOperations(const std::shared_ptr<IR::Operations::Operation>& opt, MIRFrame& frame);
        MIRFrame& processBlockInvocation(IR::Operations::BasicBlockInvocation&, MIRFrame& frame);

      private:
        struct Functions {
            MIR_item_t address;
            MIR_item_t proto;
            std::string name;
            std::string protoName;
        };
        MIR_context_t ctx;
        MIR_module_t* m;
        MIR_item_t func;
        std::shared_ptr<IR::IRGraph> ir;
        std::set<std::string> activeBlocks;
        std::map<std::string, MIR_insn_t> labels;
        std::map<std::string, Functions> functions;
        void process(const std::shared_ptr<IR::Operations::AddOperation>& opt, MIRFrame& frame);
        void process(const std::shared_ptr<IR::Operations::CastOperation>& opt, MIRFrame& frame);
        void process(const std::shared_ptr<IR::Operations::MulOperation>& mulOp, MIRFrame& frame);
        void process(const std::shared_ptr<IR::Operations::SubOperation>& subOp, MIRFrame& frame);
        void process(const std::shared_ptr<IR::Operations::IfOperation>& opt, MIRFrame& frame);
        void process(const std::shared_ptr<IR::Operations::CompareOperation>& opt, MIRFrame& frame);
        void process(const std::shared_ptr<IR::Operations::AndOperation>& opt, MIRFrame& frame);
        void process(const std::shared_ptr<IR::Operations::OrOperation>& opt, MIRFrame& frame);
        void process(const std::shared_ptr<IR::Operations::BranchOperation>& opt, MIRFrame& frame);
        void process(const std::shared_ptr<IR::Operations::LoadOperation>& opt, MIRFrame& frame);
        void process(const std::shared_ptr<IR::Operations::StoreOperation>& opt, MIRFrame& frame);
        void process(const std::shared_ptr<IR::Operations::ProxyCallOperation>& opt, MIRFrame& frame);
        MIR_reg_t getReg(const IR::Operations::OperationIdentifier& identifier, const std::shared_ptr<IR::Types::Stamp>& stamp, MIRFrame& frame);
        MIR_type_t getType(const std::shared_ptr<IR::Types::Stamp>& stamp);
    };
};

}// namespace NES::Nautilus::Backends::MIR

#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_FLOUNDER_MIRLOWERINGPROVIDER_HPP_
