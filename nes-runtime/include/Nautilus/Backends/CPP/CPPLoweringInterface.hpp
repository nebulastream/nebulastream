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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_CPP_CPPLOWERINGINTERFACE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_CPP_CPPLOWERINGINTERFACE_HPP_

#include <Nautilus/CodeGen/IRLoweringInterface.hpp>

#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Types/Stamp.hpp>
#include <Nautilus/CodeGen/CPP/Function.hpp>
#include <Nautilus/CodeGen/CPP/LabeledSegment.hpp>
#include <Nautilus/CodeGen/CPP/Statement.hpp>

#include <memory>
#include <sstream>

namespace NES::Nautilus::Backends::CPP {

class CPPLoweringInterface : public CodeGen::IRLoweringInterface {
public:
    explicit CPPLoweringInterface(const RegisterFrame& frame = RegisterFrame());

    std::unique_ptr<CodeGen::Segment> lowerGraph(const std::shared_ptr<IR::IRGraph>& irGraph);

    static std::string getType(const IR::Types::StampPtr& stamp);
    static std::string getVariable(const std::string& id);

private:
    using Code = std::unique_ptr<CodeGen::CodeGenerator>;

    std::shared_ptr<CodeGen::CPP::LabeledSegment> lowerBasicBlock(const std::shared_ptr<IR::BasicBlock>& block, RegisterFrame& frame);

    template<class T>
    Code lowerBinaryOperation(const std::shared_ptr<T>& operation, const std::string& binaryOp, RegisterFrame& frame) {
        auto leftInput = frame.getValue(operation->getLeftInput()->getIdentifier());
        auto rightInput = frame.getValue(operation->getRightInput()->getIdentifier());
        auto var = getVariable(operation->getIdentifier());
        frame.setValue(operation->getIdentifier(), var);
        auto targetType = getType(operation->getStamp());
        auto code = std::make_unique<CodeGen::Segment>();
        auto decl = std::make_shared<CodeGen::CPP::Statement>(targetType + " " + var);
        code->addToProlog(decl);
        auto stmt = std::make_shared<CodeGen::CPP::Statement>(var + " = " + leftInput + " " + binaryOp + " " + rightInput);
        code->add(stmt);
        return std::move(code);
    }

    template<class T>
    Code lowerConstOperation(const std::shared_ptr<T>& operation, RegisterFrame& frame) {
        auto var = getVariable(operation->getIdentifier());
        frame.setValue(operation->getIdentifier(), var);
        auto targetType = getType(operation->getStamp());
        std::stringstream c;
        c << var << " = " << operation->getValue();
        auto code = std::make_unique<CodeGen::Segment>();
        auto decl = std::make_shared<CodeGen::CPP::Statement>(targetType + " " + var);
        code->addToProlog(decl);
        auto stmt = std::make_shared<CodeGen::CPP::Statement>(c.str());
        code->add(stmt);
        return std::move(code);
    }

    /* IRLoweringPlugin interface */
    Code lowerAdd(const std::shared_ptr<IR::Operations::AddOperation>& operation, RegisterFrame& frame) override;
    Code lowerAddress(const std::shared_ptr<IR::Operations::AddressOperation>& operation, RegisterFrame& frame) override;
    Code lowerAnd(const std::shared_ptr<IR::Operations::AndOperation>& operation, RegisterFrame& frame) override;
    Code lowerBasicBlockArgument(const std::shared_ptr<IR::Operations::BasicBlockArgument>& operation, RegisterFrame& frame) override;
    Code lowerBasicBlockInvocation(const std::shared_ptr<IR::Operations::BasicBlockInvocation>& operation, RegisterFrame& frame) override;
    Code lowerBitwiseAnd(const std::shared_ptr<IR::Operations::BitWiseAndOperation>& operation, RegisterFrame& frame) override;
    Code lowerBitwiseLeftShift(const std::shared_ptr<IR::Operations::BitWiseLeftShiftOperation>& operation, RegisterFrame& frame) override;
    Code lowerBitwiseOr(const std::shared_ptr<IR::Operations::BitWiseOrOperation>& operation, RegisterFrame& frame) override;
    Code lowerBitwiseRightShift(const std::shared_ptr<IR::Operations::BitWiseRightShiftOperation>& operation, RegisterFrame& frame) override;
    Code lowerBitwiseXor(const std::shared_ptr<IR::Operations::BitWiseXorOperation>& operation, RegisterFrame& frame) override;
    Code lowerBranch(const std::shared_ptr<IR::Operations::BranchOperation>& operation, RegisterFrame& frame) override;
    Code lowerBuiltInVariable(const std::shared_ptr<IR::Operations::BuiltInVariableOperation>& operation, RegisterFrame& frame) override;
    Code lowerCast(const std::shared_ptr<IR::Operations::CastOperation>& operation, RegisterFrame& frame) override;
    Code lowerCompare(const std::shared_ptr<IR::Operations::CompareOperation>& operation, RegisterFrame& frame) override;
    Code lowerConstAddress(const std::shared_ptr<IR::Operations::ConstAddressOperation>& operation, RegisterFrame& frame) override;
    Code lowerConstBoolean(const std::shared_ptr<IR::Operations::ConstBooleanOperation>& operation, RegisterFrame& frame) override;
    Code lowerConstFloat(const std::shared_ptr<IR::Operations::ConstFloatOperation>& operation, RegisterFrame& frame) override;
    Code lowerConstInt(const std::shared_ptr<IR::Operations::ConstIntOperation>& operation, RegisterFrame& frame) override;
    Code lowerDiv(const std::shared_ptr<IR::Operations::DivOperation>&operation, RegisterFrame& frame) override;
    Code lowerFunction(const std::shared_ptr<IR::Operations::FunctionOperation>&operation, RegisterFrame& frame) override;
    Code lowerIf(const std::shared_ptr<IR::Operations::IfOperation>& operation, RegisterFrame& frame) override;
    Code lowerMod(const std::shared_ptr<IR::Operations::ModOperation>&operation, RegisterFrame& frame) override;
    Code lowerMul(const std::shared_ptr<IR::Operations::MulOperation>& operation, RegisterFrame& frame) override;
    Code lowerNegate(const std::shared_ptr<IR::Operations::NegateOperation>& operation, RegisterFrame& frame) override;
    Code lowerOr(const std::shared_ptr<IR::Operations::OrOperation>& operation, RegisterFrame& frame) override;
    Code lowerLoad(const std::shared_ptr<IR::Operations::LoadOperation>& operation, RegisterFrame& frame) override;
    Code lowerLoop(const std::shared_ptr<IR::Operations::LoopOperation>& operation, RegisterFrame& frame) override;
    Code lowerProxyCall(const std::shared_ptr<IR::Operations::ProxyCallOperation>& operation, RegisterFrame& frame) override;
    Code lowerReturn(const std::shared_ptr<IR::Operations::ReturnOperation>& operation, RegisterFrame& frame) override;
    Code lowerStore(const std::shared_ptr<IR::Operations::StoreOperation>& operation, RegisterFrame& frame) override;
    Code lowerSub(const std::shared_ptr<IR::Operations::SubOperation>&operation, RegisterFrame& frame) override;

    std::unordered_map<std::string, std::shared_ptr<CodeGen::CodeGenerator>> activeBlocks;
    std::unique_ptr<CodeGen::Segment> functionBody;

    RegisterFrame registerFrame;
};

} // namespace NES::Nautilus::Backends::CPP

#endif // NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_CPP_CPPLOWERINGINTERFACE_HPP_
