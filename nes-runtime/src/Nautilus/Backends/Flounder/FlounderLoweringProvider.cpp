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

#include <Nautilus/Backends/Flounder/FlounderLoweringProvider.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/CastOperation.hpp>
#include <Nautilus/IR/Operations/ConstBooleanOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LoadOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Operations/ReturnOperation.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Util/DumpHelper.hpp>
#include <Util/Logger/Logger.hpp>
#include <flounder/compilation/compiler.h>
#include <flounder/executable.h>
#include <flounder/ir/instructions.h>
#include <flounder/ir/label.h>
#include <flounder/ir/register.h>
#include <flounder/program.h>
#include <flounder/statement.h>
#include <memory>
#include <sstream>
#include <utility>
namespace NES::Nautilus::Backends::Flounder {

FlounderLoweringProvider::FlounderLoweringProvider() = default;

std::unique_ptr<flounder::Executable> FlounderLoweringProvider::lower(std::shared_ptr<IR::IRGraph> ir,
                                                                      const NES::DumpHelper& dumpHelper) {
    flounder::Compiler compiler = flounder::Compiler{/*do not optimize*/ false,
                                                     /*collect the asm code to print later*/ true};
    auto ctx = LoweringContext(std::move(ir));
    return ctx.process(compiler, dumpHelper);
}

FlounderLoweringProvider::LoweringContext::LoweringContext(std::shared_ptr<IR::IRGraph> ir) : ir(std::move(ir)) {}

std::unique_ptr<flounder::Executable> FlounderLoweringProvider::LoweringContext::process(flounder::Compiler& compiler,
                                                                                         const NES::DumpHelper& dumpHelper) {

    auto root = ir->getRootOperation();
    this->process(root);
    auto executable = std::make_unique<flounder::Executable>();
    const auto flounder_code = program.code();
    std::stringstream flounderCode;
    flounderCode << "\n == Flounder Code == \n";
    for (const auto& line : flounder_code) {
        flounderCode << line << std::endl;
    }
    dumpHelper.dump("3. flounder.ir", flounderCode.str());
    const auto isCompilationSuccessful = compiler.compile(program, *executable.get());
    if (isCompilationSuccessful) {
        NES_THROW_RUNTIME_ERROR("Flounder compilation failed!");
    }
    if (executable->compilate().has_code()) {
        std::stringstream flounderASM;
        for (const auto& line : executable->compilate().code()) {
            flounderASM << line << std::endl;
        }
        dumpHelper.dump("3. flounder.asm", flounderASM.str());
    }
    return executable;
}

void FlounderLoweringProvider::LoweringContext::process(
    const std::shared_ptr<IR::Operations::FunctionOperation>& functionOperation) {
    FlounderFrame rootFrame;
    auto functionBasicBlock = functionOperation->getFunctionBasicBlock();
    for (auto i = 0ull; i < functionBasicBlock->getArguments().size(); i++) {
        auto argument = functionBasicBlock->getArguments()[i];
        auto arg = createVreg(argument->getIdentifier(), argument->getStamp(), rootFrame);

        const auto intStamp = std::static_pointer_cast<IR::Types::IntegerStamp>(argument->getStamp());
        if (intStamp->getBitWidth() != IR::Types::IntegerStamp::BitWidth::I64) {
            auto tmpArg = program.vreg("temp_" + argument->getIdentifier());
            program << program.request_vreg64(tmpArg) << program.get_argument(i, tmpArg) << program.mov(arg, tmpArg);
        } else {
            program << program.get_argument(i, arg);
        }
    }

    this->process(functionBasicBlock, rootFrame);

    auto blockLabel = program.label("Block_return");
    program << program.section(blockLabel);
}

void FlounderLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::BasicBlock>& block, FlounderFrame& frame) {

    if (!this->activeBlocks.contains(block->getIdentifier())) {
        this->activeBlocks.emplace(block->getIdentifier());
        // FlounderFrame blockFrame;
        auto blockLabel = program.label("Block_" + block->getIdentifier());
        program << program.section(blockLabel);

        for (const auto& operation : block->getOperations()) {
            this->process(operation, frame);
        }
    }
}

void FlounderLoweringProvider::LoweringContext::processInline(const std::shared_ptr<IR::BasicBlock>& block,
                                                              FlounderFrame& frame) {
    for (const auto& operation : block->getOperations()) {
        this->process(operation, frame);
    }
}

flounder::VregInstruction FlounderLoweringProvider::LoweringContext::requestVreg(flounder::Register& reg,
                                                                                 const IR::Types::StampPtr& stamp) {
    if (stamp->isInteger()) {
        auto intStamp = std::static_pointer_cast<IR::Types::IntegerStamp>(stamp);
        flounder::RegisterWidth registerWidth;
        switch (intStamp->getBitWidth()) {
            case IR::Types::IntegerStamp::BitWidth::I8: registerWidth = flounder::r8; break;
            case IR::Types::IntegerStamp::BitWidth::I16: registerWidth = flounder::r16; break;
            case IR::Types::IntegerStamp::BitWidth::I32: registerWidth = flounder::r32; break;
            case IR::Types::IntegerStamp::BitWidth::I64: registerWidth = flounder::r64; break;
        }
        flounder::RegisterSignType signType = intStamp->isSigned() ? flounder::Signed : flounder::Unsigned;
        return program.request_vreg(reg, registerWidth, signType);
    } else if (stamp->isAddress()) {
        return program.request_vreg64(reg);
    }
    NES_THROW_RUNTIME_ERROR("Stamp for vreg not supported by flounder " << stamp->toString());
}

flounder::Register FlounderLoweringProvider::LoweringContext::createVreg(IR::Operations::OperationIdentifier id,
                                                                         IR::Types::StampPtr stamp,
                                                                         FlounderFrame& frame) {
    auto result = program.vreg(id.c_str());
    frame.setValue(id, result);
    program << requestVreg(result, stamp);
    return result;
}

FlounderLoweringProvider::FlounderFrame
FlounderLoweringProvider::LoweringContext::processBlockInvocation(IR::Operations::BasicBlockInvocation& bi,
                                                                  FlounderFrame& frame) {

    FlounderFrame blockFrame;
    auto blockInputArguments = bi.getArguments();
    auto blockTargetArguments = bi.getBlock()->getArguments();
    for (uint64_t i = 0; i < blockInputArguments.size(); i++) {
        auto blockArgument = blockInputArguments[i]->getIdentifier();
        auto blockTargetArgument = blockTargetArguments[i]->getIdentifier();
        auto frameFlounderValue = frame.getValue(blockArgument);
        if (frame.contains(blockTargetArgument)) {
            auto targetFrameFlounderValue = frame.getValue(blockTargetArgument);
            program << program.mov(targetFrameFlounderValue, frameFlounderValue);
            blockFrame.setValue(blockTargetArgument, frameFlounderValue);
        } else {
            auto targetVreg = program.vreg(blockTargetArgument.data());
            program << requestVreg(targetVreg, blockTargetArguments[i]->getStamp());
            program << program.mov(targetVreg, frameFlounderValue);
            blockFrame.setValue(blockTargetArgument, targetVreg);
        }
    }
    return blockFrame;
}

FlounderLoweringProvider::FlounderFrame
FlounderLoweringProvider::LoweringContext::processInlineBlockInvocation(IR::Operations::BasicBlockInvocation& bi,
                                                                        FlounderFrame& frame) {
    FlounderFrame blockFrame;
    auto blockInputArguments = bi.getArguments();
    auto blockTargetArguments = bi.getBlock()->getArguments();
    auto inputArguments = std::set<std::string>();
    for (uint64_t i = 0; i < blockInputArguments.size(); i++) {
        inputArguments.emplace(blockInputArguments[i]->getIdentifier());
        blockFrame.setValue(blockTargetArguments[i]->getIdentifier(), frame.getValue(blockInputArguments[i]->getIdentifier()));
    }
    return blockFrame;
}
void FlounderLoweringProvider::LoweringContext::process(
    const std::shared_ptr<IR::Operations::CastOperation>& castOp,
    NES::Nautilus::Backends::Flounder::FlounderLoweringProvider::FlounderFrame& frame) {
    auto inputReg = frame.getValue(castOp->getInput()->getIdentifier());
    auto inputStamp = castOp->getInput()->getStamp();
    auto resultReg = program.vreg(castOp->getIdentifier());
    program << requestVreg(resultReg, castOp->getStamp());
    program << program.mov(resultReg, inputReg);
    frame.setValue(castOp->getIdentifier(), resultReg);
}
void FlounderLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::AddOperation>& addOpt,
                                                        FlounderFrame& frame) {

    auto leftInput = frame.getValue(addOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());
    // perform add
    auto result = createVreg(addOpt->getIdentifier(), addOpt->getStamp(), frame);
    program << program.mov(result, leftInput);
    auto addFOp = program.add(result, rightInput);
    frame.setValue(addOpt->getIdentifier(), result);
    program << addFOp;
}

void FlounderLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::MulOperation>& mulOp,
                                                        FlounderFrame& frame) {
    auto leftInput = frame.getValue(mulOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(mulOp->getRightInput()->getIdentifier());
    auto result = createVreg(mulOp->getIdentifier(), mulOp->getStamp(), frame);
    program << program.mov(result, leftInput);
    auto addFOp = program.imul(result, rightInput);
    frame.setValue(mulOp->getIdentifier(), result);
    program << addFOp;
}

void FlounderLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::SubOperation>& subOp,
                                                        FlounderFrame& frame) {
    auto leftInput = frame.getValue(subOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(subOp->getRightInput()->getIdentifier());
    auto addFOp = program.sub(leftInput, rightInput);
    frame.setValue(subOp->getIdentifier(), leftInput);
    program << addFOp;
}

void FlounderLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::LoadOperation>& opt,
                                                        FlounderFrame& frame) {
    auto address = frame.getValue(opt->getAddress()->getIdentifier());
    auto memNode = program.mem(address);
    auto resultVreg = createVreg(opt->getIdentifier(), opt->getStamp(), frame);
    program << program.mov(resultVreg, memNode);
}

void FlounderLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::StoreOperation>& opt,
                                                        FlounderFrame& frame) {
    auto address = frame.getValue(opt->getAddress()->getIdentifier());
    auto value = frame.getValue(opt->getValue()->getIdentifier());
    auto memNode = program.mem(address);
    program << program.mov(memNode, value);
}

void FlounderLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::BranchOperation>& branchOp,
                                                        FlounderFrame& frame) {
    auto targetFrame = processInlineBlockInvocation(branchOp->getNextBlockInvocation(), frame);
    processInline(branchOp->getNextBlockInvocation().getBlock(), targetFrame);
}

void FlounderLoweringProvider::LoweringContext::processCmp(
    const std::shared_ptr<IR::Operations::Operation>& opt,
    NES::Nautilus::Backends::Flounder::FlounderLoweringProvider::FlounderFrame& frame,
    flounder::Label& trueCase,
    flounder::Label& falseCase) {
    if (opt->getOperationType() == IR::Operations::Operation::OperationType::CompareOp) {
        auto left = std::static_pointer_cast<IR::Operations::CompareOperation>(opt);
        processCmp(left, frame, falseCase);
        program << program.jmp(trueCase);
    } else if (opt->getOperationType() == IR::Operations::Operation::OperationType::AndOp) {
        auto left = std::static_pointer_cast<IR::Operations::AndOperation>(opt);
        processAnd(left, frame, trueCase, falseCase);
    } else if (opt->getOperationType() == IR::Operations::Operation::OperationType::OrOp) {
        auto left = std::static_pointer_cast<IR::Operations::OrOperation>(opt);
        processOr(left, frame, trueCase, falseCase);
    } else {
        NES_THROW_RUNTIME_ERROR("Left is not a compare operation but a " << opt->toString());
    }
}

void FlounderLoweringProvider::LoweringContext::processAnd(const std::shared_ptr<IR::Operations::AndOperation>& andOpt,
                                                           FlounderFrame& frame,
                                                           flounder::Label& trueCase,
                                                           flounder::Label& falseCase) {
    auto andSecondCaseLabel = program.label("And_tmp_label" + andOpt->getIdentifier());
    auto left = andOpt->getLeftInput();
    processCmp(left, frame, andSecondCaseLabel, falseCase);
    program << program.section(andSecondCaseLabel);
    auto right = andOpt->getRightInput();
    processCmp(right, frame, trueCase, falseCase);
    program << program.jmp(trueCase);
}

void FlounderLoweringProvider::LoweringContext::processOr(
    const std::shared_ptr<IR::Operations::OrOperation>& orOpt,
    NES::Nautilus::Backends::Flounder::FlounderLoweringProvider::FlounderFrame& frame,
    flounder::Label& trueCase,
    flounder::Label& falseCase) {
    auto orSecondCaseLabel = program.label("Or_tmp_label" + orOpt->getIdentifier());
    auto left = orOpt->getLeftInput();
    processCmp(left, frame, trueCase, orSecondCaseLabel);
    program << program.jmp(trueCase);
    program << program.section(orSecondCaseLabel);
    auto right = orOpt->getRightInput();
    processCmp(right, frame, trueCase, falseCase);
    program << program.jmp(trueCase);
}

void FlounderLoweringProvider::LoweringContext::processCmp(const std::shared_ptr<IR::Operations::CompareOperation>& compOpt,
                                                           FlounderFrame& frame,
                                                           flounder::Label& falseCase) {
    auto leftInput = frame.getValue(compOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(compOpt->getRightInput()->getIdentifier());
    program << program.cmp(leftInput, rightInput);
    switch (compOpt->getComparator()) {
        case IR::Operations::CompareOperation::Comparator::EQ: program << program.jne(falseCase); break;
        case IR::Operations::CompareOperation::Comparator::LT: program << program.jge(falseCase); break;
        case IR::Operations::CompareOperation::Comparator::GE: program << program.jl(falseCase); break;
        case IR::Operations::CompareOperation::Comparator::LE: program << program.jg(falseCase); break;
        case IR::Operations::CompareOperation::Comparator::GT: program << program.jle(falseCase); break;
        case IR::Operations::CompareOperation::Comparator::NE: program << program.je(falseCase); break;
        default: NES_THROW_RUNTIME_ERROR("No handler for comp");
    }
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::IfOperation> ifOpt,
                                                        FlounderFrame& frame) {
    auto trueLabel = program.label("Block_" + ifOpt->getTrueBlockInvocation().getBlock()->getIdentifier());
    auto falseLabel = program.label("Block_" + ifOpt->getFalseBlockInvocation().getBlock()->getIdentifier());
    // clear all non args
    auto falseBlockFrame = processBlockInvocation(ifOpt->getFalseBlockInvocation(), frame);
    auto trueBlockFrame = processBlockInvocation(ifOpt->getTrueBlockInvocation(), frame);

    auto booleanValue = ifOpt->getValue();
    processCmp(booleanValue, frame, trueLabel, falseLabel);

    process(ifOpt->getTrueBlockInvocation().getBlock(), trueBlockFrame);
    process(ifOpt->getFalseBlockInvocation().getBlock(), falseBlockFrame);
}

void FlounderLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::ProxyCallOperation>& opt,
                                                        FlounderFrame& frame) {

    std::vector<flounder::Register> callArguments;
    for (const auto& arg : opt->getInputArguments()) {
        auto value = frame.getValue(arg->getIdentifier());
        callArguments.emplace_back(value);
    }

    if (!opt->getStamp()->isVoid()) {
        auto resultRegister = createVreg(opt->getIdentifier(), opt->getStamp(), frame);
        auto call = program.fcall((std::uintptr_t) opt->getFunctionPtr(), resultRegister);
        call.arguments().assign(callArguments.begin(), callArguments.end());
        program << call;
    } else {
        auto call = program.fcall((std::uintptr_t) opt->getFunctionPtr());
        call.arguments().assign(callArguments.begin(), callArguments.end());
        program << call;
    }
}

void FlounderLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::LoopOperation>& opt,
                                                        FlounderFrame& frame) {
    auto loopHead = opt->getLoopHeadBlock();
    auto targetFrame = processInlineBlockInvocation(loopHead, frame);
    processInline(loopHead.getBlock(), targetFrame);
}

void FlounderLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::Operation>& opt,
                                                        FlounderFrame& frame) {
    switch (opt->getOperationType()) {
        case IR::Operations::Operation::OperationType::ConstBooleanOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(opt);
            auto flounderConst = program.constant64(constInt->getValue() ? 1 : 0);
            return;
        }
        case IR::Operations::Operation::OperationType::ConstIntOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstIntOperation>(opt);
            auto flounderConst = program.constant64(constInt->getValue());
            auto resultVreg = createVreg(opt->getIdentifier(), opt->getStamp(), frame);
            program << program.mov(resultVreg, flounderConst);
            frame.setValue(constInt->getIdentifier(), resultVreg);
            return;
        }
        case IR::Operations::Operation::OperationType::AddOp: {
            auto addOpt = std::static_pointer_cast<IR::Operations::AddOperation>(opt);
            process(addOpt, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::MulOp: {
            auto mulOpt = std::static_pointer_cast<IR::Operations::MulOperation>(opt);
            process(mulOpt, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::SubOp: {
            auto subOpt = std::static_pointer_cast<IR::Operations::SubOperation>(opt);
            process(subOpt, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::CompareOp: {
            auto compOpt = std::static_pointer_cast<IR::Operations::CompareOperation>(opt);
            process(compOpt, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::IfOp: {
            auto ifOpt = std::static_pointer_cast<IR::Operations::IfOperation>(opt);
            process(ifOpt, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::ReturnOp: {
            auto returnOpt = std::static_pointer_cast<IR::Operations::ReturnOperation>(opt);
            if (returnOpt->hasReturnValue()) {
                auto returnFOp = frame.getValue(returnOpt->getReturnValue()->getIdentifier());
                program << program.set_return(returnFOp);
            }
            auto branchLabel = program.label("Block_return");
            program << program.jmp(branchLabel);
            return;
        }
        case IR::Operations::Operation::OperationType::BranchOp: {
            auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(opt);
            process(branchOp, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::LoopOp: {
            auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(opt);
            process(loopOp, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::LoadOp: {
            auto load = std::static_pointer_cast<IR::Operations::LoadOperation>(opt);
            process(load, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::StoreOp: {
            auto store = std::static_pointer_cast<IR::Operations::StoreOperation>(opt);
            process(store, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::ProxyCallOp: {
            auto call = std::static_pointer_cast<IR::Operations::ProxyCallOperation>(opt);
            process(call, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::OrOp: {
            auto call = std::static_pointer_cast<IR::Operations::OrOperation>(opt);
            process(call, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::AndOp: {
            auto call = std::static_pointer_cast<IR::Operations::AndOperation>(opt);
            process(call, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::CastOp: {
            auto cast = std::static_pointer_cast<IR::Operations::CastOperation>(opt);
            process(cast, frame);
            return;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("Operation " << opt->toString() << " not handled");
        }
    }
}

}// namespace NES::Nautilus::Backends::Flounder