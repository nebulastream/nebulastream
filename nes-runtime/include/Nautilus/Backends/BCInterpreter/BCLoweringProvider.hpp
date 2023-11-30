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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BCLOWERINGPROVIDER_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BCLOWERINGPROVIDER_HPP_

#include <Nautilus/Backends/BCInterpreter/ByteCode.hpp>
#include <Nautilus/Backends/MLIR/ProxyFunctions.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/AddressOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/CastOperation.hpp>
#include <Nautilus/IR/Operations/ConstBooleanOperation.hpp>
#include <Nautilus/IR/Operations/ConstFloatOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LoadOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Operations/ProxyCallOperation.hpp>
#include <Nautilus/IR/Operations/ReturnOperation.hpp>
#include <Nautilus/IR/Operations/StoreOperation.hpp>
#include <Nautilus/Util/Frame.hpp>
#include <unordered_set>

namespace NES::Nautilus::Backends::BC {

/**
 * @brief The lowering provider translates the IR to the bytecode.
 */
class BCLoweringProvider {
  public:
    BCLoweringProvider();
    std::tuple<Code, RegisterFile> lower(std::shared_ptr<IR::IRGraph> ir);

  private:
    using RegisterFrame = Frame<std::string, short>;
    class RegisterProvider {
      public:
        short allocRegister();
        void freeRegister();

      private:
        short currentRegister = 0;
    };
    class LoweringContext {
      public:
        LoweringContext(std::shared_ptr<IR::IRGraph> ir);
        std::tuple<Code, RegisterFile> process();
        short process(const std::shared_ptr<IR::BasicBlock>&, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::Operation>& operation, short block, RegisterFrame& frame);

      private:
        Code program;
        RegisterFile defaultRegisterFile;
        std::shared_ptr<IR::IRGraph> ir;
        RegisterProvider registerProvider;
        std::unordered_map<std::string, short> activeBlocks;
        void process(const std::shared_ptr<IR::Operations::AddOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::MulOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::DivOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::SubOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::IfOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::CompareOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::BranchOperation>& opt, short block, RegisterFrame& frame);
        void process(IR::Operations::BasicBlockInvocation& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::LoadOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::StoreOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::ProxyCallOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::OrOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::AndOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::NegateOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::CastOperation>& opt, short block, RegisterFrame& frame);
        bool processNativeCall(const std::shared_ptr<IR::Operations::ProxyCallOperation>& opt, short block, RegisterFrame& frame);
        void
        processDynamicCall(const std::shared_ptr<IR::Operations::ProxyCallOperation>& opt, short block, RegisterFrame& frame);
        short getResultRegister(const std::shared_ptr<IR::Operations::Operation>& opt, RegisterFrame& frame);
    };
};
}// namespace NES::Nautilus::Backends::BC
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BCLOWERINGPROVIDER_HPP_
