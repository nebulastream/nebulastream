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

#include "Nautilus/IR/Operations/CastOperation.hpp"
#include "Nautilus/IR/Operations/LoadOperation.hpp"
#include "Nautilus/IR/Operations/Operation.hpp"
#include "asmjit/x86/x86operand.h"
#include "flounder/ir/instructions.h"
#include "flounder/ir/label.h"
#include "flounder/ir/register.h"
#include <Nautilus/Backends/Flounder/FlounderLoweringProvider.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/ConstBooleanOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp>
#include <Nautilus/IR/Operations/ReturnOperation.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <flounder/compilation/compiler.h>
#include <flounder/executable.h>
#include <flounder/program.h>
#include <flounder/statement.h>
#include <memory>
namespace NES::Nautilus::Backends::Flounder {

FlounderLoweringProvider::FlounderLoweringProvider(){};

std::unique_ptr<flounder::Executable> FlounderLoweringProvider::lower(std::shared_ptr<IR::IRGraph> ir) {
    auto ctx = LoweringContext(ir);
    return ctx.process(compiler);
}

FlounderLoweringProvider::LoweringContext::LoweringContext(std::shared_ptr<IR::IRGraph> ir) : ir(ir) {}

std::unique_ptr<flounder::Executable> FlounderLoweringProvider::LoweringContext::process(flounder::Compiler& compiler) {

    auto root = ir->getRootOperation();
    this->process(root);

    auto executable = std::make_unique<flounder::Executable>();

    const auto flounder_code = program.code();
    std::cout << "\n == Flounder Code == \n";
    for (const auto& line : flounder_code) {
        std::cout << line << std::endl;
    }
    const auto is_compilation_successful = compiler.compile(program, *executable.get());
    if (executable->compilate().has_code())
        for (const auto& line : executable->compilate().code()) {
            std::cout << line << std::endl;
        }
    return executable;
}

void FlounderLoweringProvider::LoweringContext::process(
    const std::shared_ptr<IR::Operations::FunctionOperation>& functionOperation) {
    FlounderFrame rootFrame;
    auto functionBasicBlock = functionOperation->getFunctionBasicBlock();
    for (auto i = 0ull; i < functionBasicBlock->getArguments().size(); i++) {
        auto argument = functionBasicBlock->getArguments()[i];
        // auto arg = program.vreg(argument->getIdentifier());
        // rootFrame.setValue(argument->getIdentifier(), arg);
        auto arg = createVreg(argument->getIdentifier(), argument->getStamp(), rootFrame);

        auto intStamp = std::static_pointer_cast<IR::Types::IntegerStamp>(argument->getStamp());
        flounder::RegisterWidth registerWidth;
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

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::BasicBlock> block, FlounderFrame& parentFrame) {

    if (!this->activeBlocks.contains(block->getIdentifier())) {
        this->activeBlocks.emplace(block->getIdentifier());
        // FlounderFrame blockFrame;
        auto blockLabel = program.label("Block_" + block->getIdentifier());
        program << program.section(blockLabel);

        for (const auto& opt : block->getOperations()) {
            this->process(opt, parentFrame);
        }

        //for (auto& item : parentFrame.getContent()) {
        //    if (item.second->type() == flounder::InstructionType::RequestVreg) {
        //auto vregNode = static_cast<flounder::VregInstruction*>(item.second);
        // program << program.clear(vregNode);
        //    }
        // }
    }
}

void FlounderLoweringProvider::LoweringContext::processInline(std::shared_ptr<IR::BasicBlock> block, FlounderFrame& parentFrame) {

    //if (!this->activeBlocks.contains(block->getIdentifier())) {
    //   this->activeBlocks.emplace(block->getIdentifier());
    // FlounderFrame blockFrame;
    //auto blockLabel = program.label("Block_" + block->getIdentifier());
    //program << program.section(blockLabel);

    for (auto opt : block->getOperations()) {
        this->process(opt, parentFrame);
    }

    for (auto& item : parentFrame.getContent()) {
        //if (item.second->type() == flounder::InstructionType::RequestVreg) {
        //auto vregNode = static_cast<flounder::VirtualRegisterIdentifierNode*>(item.second);
        //      program << program.clear(vregNode);
        // }
    }
    // }
}

flounder::VregInstruction FlounderLoweringProvider::LoweringContext::requestVreg(flounder::Register& reg,
                                                                                 IR::Types::StampPtr stamp) {
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
                                                                  FlounderFrame& parentFrame) {

    FlounderFrame blockFrame;
    auto blockInputArguments = bi.getArguments();
    auto blockTargetArguments = bi.getBlock()->getArguments();
    for (uint64_t i = 0; i < blockInputArguments.size(); i++) {
        auto blockArgument = blockInputArguments[i]->getIdentifier();
        auto blockTargetArgument = blockTargetArguments[i]->getIdentifier();
        auto parentFrameFlounderValue = parentFrame.getValue(blockArgument);
        // if the value is no a vrec, we have to materialize it before invoicing the subframe.
        //if (parentFrameFlounderValue->type() != flounder::VREG) {
        //    auto argumentVreg = program.vreg(blockArgument.data());
        //    program << program.request_vreg64(argumentVreg);
        //    program << program.mov(argumentVreg, parentFrameFlounderValue);
        //    parentFrameFlounderValue = argumentVreg;
        //}

        if (parentFrame.contains(blockTargetArgument.data())) {
            auto targetFrameFlounderValue = parentFrame.getValue(blockTargetArgument.data());
            program << program.mov(targetFrameFlounderValue, parentFrameFlounderValue);
            blockFrame.setValue(blockTargetArgument.data(), parentFrameFlounderValue);
        } else {
            auto targetVreg = program.vreg(blockTargetArgument.data());
            program << requestVreg(targetVreg, blockTargetArguments[i]->getStamp());
            // program << program.request_vreg64(targetVreg);
            program << program.mov(targetVreg, parentFrameFlounderValue);
            blockFrame.setValue(blockTargetArgument.data(), targetVreg);
        }
    }
    return blockFrame;
}

FlounderLoweringProvider::FlounderFrame
FlounderLoweringProvider::LoweringContext::processInlineBlockInvocation(IR::Operations::BasicBlockInvocation& bi,
                                                                        FlounderFrame& parentFrame) {

    FlounderFrame blockFrame;
    auto blockInputArguments = bi.getArguments();
    auto blockTargetArguments = bi.getBlock()->getArguments();
    auto inputArguments = std::set<std::string>();
    for (uint64_t i = 0; i < blockInputArguments.size(); i++) {
        inputArguments.emplace(blockInputArguments[i]->getIdentifier());
        blockFrame.setValue(blockTargetArguments[i]->getIdentifier(),
                            parentFrame.getValue(blockInputArguments[i]->getIdentifier()));
    }
    //for (auto& item : parentFrame.getContent()) {
    //  if (!inputArguments.contains(item.first) && item.second->type() == flounder::InstructionType::VREG) {
    //       auto vregNode = static_cast<flounder::VirtualRegisterIdentifierNode*>(item.second);
    // program << program.clear(vregNode);
    //  }
    //}
    return blockFrame;
}
void FlounderLoweringProvider::LoweringContext::process(
    std::shared_ptr<IR::Operations::CastOperation> castOp,
    NES::Nautilus::Backends::Flounder::FlounderLoweringProvider::FlounderFrame& frame) {
    auto inputReg = frame.getValue(castOp->getInput()->getIdentifier());
    auto inputStamp = castOp->getInput()->getStamp();
    auto resultReg = program.vreg(castOp->getIdentifier());
    //program << requestVreg(inputReg, inputStamp);
    program << requestVreg(resultReg, castOp->getStamp());
    program << program.mov(resultReg, inputReg);
    frame.setValue(castOp->getIdentifier(), resultReg);
}
void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::AddOperation> addOpt,
                                                        FlounderFrame& frame) {

    auto leftInput = addOpt->getLeftInput();
    auto leftFlounderRef = frame.getValue(leftInput->getIdentifier());
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());

    //if (leftInput->getStamp()->isAddress() && addOpt->getUsages().size() == 1
    //    && addOpt->getUsages()[0]->getOperationType() == IR::Operations::Operation::OperationType::LoadOp) {
    //    auto result = program.mem(leftFlounderRef, rightInput);
    //    frame.setValue(addOpt->getIdentifier(), result);
    //    return;
    //}

    //if (leftInput->getUsages().size() > 1 || leftFlounderRef->type() != flounder::VREG) {
    // the operation has shared access to the left input -> create new result register
    //    auto result = createVreg(addOpt->getIdentifier(), addOpt->getStamp(), frame);
    //    program << program.mov(result, leftFlounderRef);
    //    leftFlounderRef = result;
    //}

    // perform add
    auto result = createVreg(addOpt->getIdentifier(), addOpt->getStamp(), frame);
    program << program.mov(result, leftFlounderRef);

    auto addFOp = program.add(result, rightInput);
    frame.setValue(addOpt->getIdentifier(), result);
    program << addFOp;

    // clear registers if we dont used them
    //if (addOpt->getRightInput()->getUsages().size() == 1 && rightInput->type() == flounder::VREG) {
    //program << program.clear(static_cast<flounder::VirtualRegisterIdentifierNode*>(rightInput));
    // }
    return;
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::MulOperation> addOpt,
                                                        FlounderFrame& frame) {
    auto leftInput = addOpt->getLeftInput();
    flounder::Register leftFlounderRef = frame.getValue(leftInput->getIdentifier());

    //if (leftInput->getUsages().size() > 1 || leftFlounderRef->type() != flounder::VREG) {
    // the operation has shared access to the left input -> create new result register
    //    auto result = createVreg(addOpt->getIdentifier(), addOpt->getStamp(), frame);
    //    program << program.mov(result, leftFlounderRef);
    //    leftFlounderRef = result;
    //}
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());
    auto result = createVreg(addOpt->getIdentifier(), addOpt->getStamp(), frame);
    program << program.mov(result, leftFlounderRef);

    auto addFOp = program.imul(result, rightInput);
    frame.setValue(addOpt->getIdentifier(), result);
    program << addFOp;
    // clear registers if we dont used them
    //if (addOpt->getRightInput()->getUsages().size() == 1 && rightInput->type() == flounder::VREG) {
    // program << program.clear(static_cast<flounder::VirtualRegisterIdentifierNode*>(rightInput));
    //}
    return;
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::SubOperation> addOpt,
                                                        FlounderFrame& frame) {
    auto leftInput = addOpt->getLeftInput();
    flounder::Register leftFlounderRef = frame.getValue(leftInput->getIdentifier());

    //if (leftInput->getUsages().size() > 1 || leftFlounderRef->type() != flounder::VREG) {
    // the operation has shared access to the left input -> create new result register
    //   auto result = createVreg(addOpt->getIdentifier(), addOpt->getStamp(), frame);
    //     program << program.mov(result, leftFlounderRef);
    //    leftFlounderRef = result;
    // }
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());

    auto addFOp = program.sub(leftFlounderRef, rightInput);
    frame.setValue(addOpt->getIdentifier(), leftFlounderRef);
    program << addFOp;
    // clear registers if we dont used them
    //if (addOpt->getRightInput()->getUsages().size() == 1 && rightInput->type() == flounder::VREG) {
    // program << program.clear(static_cast<flounder::VirtualRegisterIdentifierNode*>(rightInput));
    // }
    return;
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::CompareOperation>, FlounderFrame&) {

    // if (leftInput->type() != flounder::VREG) {

    //     program << program.clear(tempVreg);
    // } else {
    //     program << program.cmp(leftInput, rightInput);
    // }
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::OrOperation>, FlounderFrame&) {}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::AndOperation>, FlounderFrame&) {}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::LoadOperation> opt,
                                                        FlounderFrame& frame) {
    auto address = frame.getValue(opt->getAddress()->getIdentifier());
    auto memNode = program.mem(address);
    auto resultVreg = createVreg(opt->getIdentifier(), opt->getStamp(), frame);
    program << program.mov(resultVreg, memNode);
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::StoreOperation> opt,
                                                        FlounderFrame& frame) {
    auto address = frame.getValue(opt->getAddress()->getIdentifier());
    auto value = frame.getValue(opt->getValue()->getIdentifier());
    auto memNode = program.mem(address);
    program << program.mov(memNode, value);
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::BranchOperation> branchOp,
                                                        FlounderFrame& frame) {
    auto branchLabel = program.label("Block_" + branchOp->getNextBlockInvocation().getBlock()->getIdentifier());
    auto targetFrame = processInlineBlockInvocation(branchOp->getNextBlockInvocation(), frame);
    //program << program.jmp(branchLabel);
    processInline(branchOp->getNextBlockInvocation().getBlock(), targetFrame);
}

void FlounderLoweringProvider::LoweringContext::processCmp(
    std::shared_ptr<IR::Operations::Operation> opt,
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
    } else if (opt->getOperationType() == IR::Operations::Operation::OperationType::ConstBooleanOp) {

    } else {
        NES_THROW_RUNTIME_ERROR("Left is not a compare operation but a " << opt->toString());
    }
}

void FlounderLoweringProvider::LoweringContext::processAnd(std::shared_ptr<IR::Operations::AndOperation> andOpt,
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
    std::shared_ptr<IR::Operations::OrOperation> orOpt,
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

void FlounderLoweringProvider::LoweringContext::processCmp(std::shared_ptr<IR::Operations::CompareOperation> compOpt,
                                                           FlounderFrame& frame,
                                                           flounder::Label& falseCase) {
    auto leftInput = frame.getValue(compOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(compOpt->getRightInput()->getIdentifier());
    //auto tempVreg = createVreg(compOpt->getIdentifier(), compOpt->getStamp(), frame);
    //program << program.mov(tempVreg, leftInput);
    program << program.cmp(leftInput, rightInput);

    switch (compOpt->getComparator()) {
        case IR::Operations::CompareOperation::Comparator::IEQ: program << program.jne(falseCase); break;
        case IR::Operations::CompareOperation::Comparator::ISLT: program << program.jge(falseCase); break;
        case IR::Operations::CompareOperation::Comparator::ISGE: program << program.jl(falseCase); break;
        case IR::Operations::CompareOperation::Comparator::ISLE: program << program.jg(falseCase); break;
        case IR::Operations::CompareOperation::Comparator::ISGT: program << program.jle(falseCase); break;
        case IR::Operations::CompareOperation::Comparator::INE: program << program.je(falseCase); break;
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
    /*for (auto& item : trueBlockFrame.getContent()) {
        if (item.second->type() == flounder::InstructionType::VREG) {
            auto vregNode = static_cast<flounder::VirtualRegisterIdentifierNode*>(item.second);
            program << program.clear(vregNode);
        }
    }*/
    process(ifOpt->getFalseBlockInvocation().getBlock(), falseBlockFrame);
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::ProxyCallOperation> opt,
                                                        FlounderFrame& frame) {

    std::vector<flounder::Register> callArguments;
    for (const auto& arg : opt->getInputArguments()) {
        auto value = frame.getValue(arg->getIdentifier());
        // we first have to materialize constant in a register
        //if (value->type() == flounder::InstructionType::CONSTANT) {
        //    auto* resultRegister = program.vreg(arg->getIdentifier());
        //    program << program.request_vreg64(resultRegister);
        //    program << program.mov(resultRegister, value);
        //    callArguments.emplace_back(resultRegister);
        //} else {
        callArguments.emplace_back(value);
        //}
    }

    if (!opt->getStamp()->isVoid()) {
        // auto resultRegister = program.vreg(opt->getIdentifier());
        //program << program.request_vreg64(resultRegister);

        //auto resultRegister = createVreg(opt->getIdentifier(), opt->getStamp(), frame);

        //auto call = flounder::FunctionCall(program, (std::uintptr_t) opt->getFunctionPtr(), opt->getIdentifier());

        // frame.setValue(opt->getIdentifier(), resultRegister);
        //call.call();
        // call.call({callArguments.begin(), callArguments.end()});

        auto resultRegister = createVreg(opt->getIdentifier(), opt->getStamp(), frame);
        auto call = program.fcall((std::uintptr_t) opt->getFunctionPtr(), resultRegister);
        // frame.setValue(opt->getIdentifier(), resultRegister);
        //for(auto& arg: callArguments){
        //    call.arguments().emplace_back(arg);
        //}
        call.arguments().assign(callArguments.begin(), callArguments.end());
        program << call;
    } else {
        auto call = program.fcall((std::uintptr_t) opt->getFunctionPtr());
        call.arguments().assign(callArguments.begin(), callArguments.end());
        program << call;
    }
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::LoopOperation> opt,
                                                        FlounderFrame& frame) {

    auto loopHead = opt->getLoopHeadBlock();
    auto targetFrame = processInlineBlockInvocation(loopHead, frame);
    //program << program.jmp(branchLabel);
    processInline(loopHead.getBlock(), targetFrame);
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::Operation> opt, FlounderFrame& frame) {
    switch (opt->getOperationType()) {
        case IR::Operations::Operation::OperationType::ConstBooleanOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(opt);
            auto flounderConst = program.constant64(constInt->getValue() ? 1 : 0);
            // frame.setValue(constInt->getIdentifier(), flounderConst);
            return;
        }
        case IR::Operations::Operation::OperationType::ConstIntOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstIntOperation>(opt);
            auto flounderConst = program.constant64(constInt->getConstantIntValue());
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
                //if (returnFOp->type() == flounder::InstructionType::RequestVreg) {
                //program << program.clear(static_cast<flounder::VirtualRegisterIdentifierNode*>(returnFOp));
                //  }
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