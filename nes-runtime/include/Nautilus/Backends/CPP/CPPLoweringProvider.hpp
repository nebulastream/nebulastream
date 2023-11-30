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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_CPP_CPPLOWERINGPROVIDER_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_CPP_CPPLOWERINGPROVIDER_HPP_

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
#include <Nautilus/IR/Types/Stamp.hpp>
#include <Nautilus/Util/Frame.hpp>
#include <sstream>
#include <unordered_set>
#include <vector>

namespace NES::Nautilus::Backends::CPP {

/**
 * @brief The lowering provider translates the IR to the cpp code.
 */
class CPPLoweringProvider {
  public:
    CPPLoweringProvider() = default;
    static std::string lower(std::shared_ptr<IR::IRGraph> ir);

  private:
    using RegisterFrame = Frame<std::string, std::string>;
    using Code = std::stringstream;

    class LoweringContext {
      public:
        explicit LoweringContext(std::shared_ptr<IR::IRGraph> ir);
        Code process();

      private:
        Code blockArguments;
        Code functions;
        std::vector<Code> blocks;
        std::shared_ptr<IR::IRGraph> ir;
        std::unordered_map<std::string, std::string> activeBlocks;
        std::unordered_set<std::string> functionNames;
        std::string process(const std::shared_ptr<IR::BasicBlock>&, RegisterFrame& frame);
        void process(IR::Operations::BasicBlockInvocation& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::Operation>& operation, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::IfOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::CompareOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::BranchOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::LoadOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::StoreOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::ProxyCallOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::NegateOperation>& opt, short block, RegisterFrame& frame);
        void process(const std::shared_ptr<IR::Operations::CastOperation>& opt, short block, RegisterFrame& frame);
        static std::string getVariable(const std::string& id);
        static std::string getType(const IR::Types::StampPtr& stamp);

        template<class Type>
        void processConst(const std::shared_ptr<IR::Operations::Operation>& opt, short blockIndex, RegisterFrame& frame) {
            auto constValue = std::static_pointer_cast<Type>(opt);
            auto var = getVariable(constValue->getIdentifier());
            blockArguments << getType(constValue->getStamp()) << " " << var << ";\n";
            frame.setValue(constValue->getIdentifier(), var);
            blocks[blockIndex] << var << " = " << constValue->getValue() << ";\n";
        }

        template<class Type>
        void processBinary(const std::shared_ptr<IR::Operations::Operation>& o,
                           const std::string& operation,
                           short blockIndex,
                           RegisterFrame& frame) {
            auto op = std::static_pointer_cast<Type>(o);
            auto leftInput = frame.getValue(op->getLeftInput()->getIdentifier());
            auto rightInput = frame.getValue(op->getRightInput()->getIdentifier());
            auto resultVar = getVariable(op->getIdentifier());
            blockArguments << getType(op->getStamp()) << " " << resultVar << ";\n";
            frame.setValue(op->getIdentifier(), resultVar);
            blocks[blockIndex] << resultVar << " = " << leftInput << operation << rightInput << ";\n";
        }
    };
};
}// namespace NES::Nautilus::Backends::CPP
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_CPP_CPPLOWERINGPROVIDER_HPP_
