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

#include "Nautilus/IR/Operations/ConstFloatOperation.hpp"
#include "Nautilus/IR/Types/StampFactory.hpp"
#include <Nautilus/Backends/MIR/MIRLoweringProvider.hpp>
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
#include <Nautilus/IR/Types/FloatStamp.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Util/DumpHelper.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <cstdint>
#include <flounder/compilation/compiler.h>
#include <flounder/executable.h>
#include <flounder/ir/instructions.h>
#include <flounder/ir/label.h>
#include <flounder/ir/register.h>
#include <flounder/program.h>
#include <flounder/statement.h>
#include <memory>
#include <mir.h>
#include <sstream>
#include <utility>
#include <vector>
namespace NES::Nautilus::Backends::MIR {

MIRLoweringProvider::MIRLoweringProvider() = default;

MIR_item_t MIRLoweringProvider::lower(std::shared_ptr<IR::IRGraph> ir,
                                      const NES::DumpHelper& dumpHelper,
                                      MIR_context_t ctx,
                                      MIR_module_t* m) {

    auto low = LoweringContext(ctx, m, std::move(ir));
    return low.process(dumpHelper);
}

MIRLoweringProvider::LoweringContext::LoweringContext(MIR_context_t ctx, MIR_module_t* m, std::shared_ptr<IR::IRGraph> ir)
    : ctx(ctx), m(m), ir(std::move(ir)) {}

MIR_item_t MIRLoweringProvider::LoweringContext::process(const NES::DumpHelper&) {

    if (m != NULL)
        *m = MIR_new_module(ctx, "m");

    auto root = ir->getRootOperation();
    this->process(root);
    MIR_finish_func (ctx);
    MIR_finish_module (ctx);
    return func;
}

void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::FunctionOperation>& functionOperation) {
    MIRFrame rootFrame;
    auto functionBasicBlock = functionOperation->getFunctionBasicBlock();
    auto arguments = functionBasicBlock->getArguments();
    std::vector<const char*> args;
    MIR_var_t* mirArgs = (MIR_var_t*) alloca(sizeof(MIR_var_t) * arguments.size());
    for (auto i = 0ull; i < arguments.size(); i++) {
        auto& argument = arguments[i];
        mirArgs[i].type = MIR_T_I64;
        mirArgs[i].name = argument->getIdentifierRef().data();
    }

    if (functionOperation->getStamp()->isVoid()) {
        auto res_type = MIR_T_I64;
        func = MIR_new_func_arr(ctx, "loop", 0, &res_type, arguments.size(), mirArgs);
    } else {
        auto res_type = getType(functionOperation->getStamp());
        func = MIR_new_func_arr(ctx, "loop", 1, &res_type, arguments.size(), mirArgs);
    }
    for (auto i = 0ull; i < arguments.size(); i++) {
        auto argument = functionBasicBlock->getArguments()[i];
        auto mirArg = MIR_reg(ctx, argument->getIdentifierRef().c_str(), func->u.func);
        rootFrame.setValue(argument->getIdentifierRef(), mirArg);
    }
    this->process(functionBasicBlock, rootFrame);
}

void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::BasicBlock>& block, MIRFrame& frame) {

    if (!this->activeBlocks.contains(block->getIdentifier())) {
        this->activeBlocks.emplace(block->getIdentifier());
        // MIRFrame blockFrame;
        if (!labels.contains(block->getIdentifier())) {
            auto label = MIR_new_label(ctx);
            labels[block->getIdentifier()] = label;
        }
        MIR_append_insn(ctx, func, labels[block->getIdentifier()]);

        for (const auto& operation : block->getOperations()) {
            this->process(operation, frame);
        }
    }
}

void MIRLoweringProvider::LoweringContext::processInline(const std::shared_ptr<IR::BasicBlock>& block, MIRFrame& frame) {
    for (const auto& operation : block->getOperations()) {
        this->process(operation, frame);
    }
}

MIRLoweringProvider::MIRFrame
MIRLoweringProvider::LoweringContext::processBlockInvocation(IR::Operations::BasicBlockInvocation& bi, MIRFrame& frame) {

    auto blockInputArguments = bi.getArguments();
    auto blockTargetArguments = bi.getBlock()->getArguments();
    for (uint64_t i = 0; i < blockInputArguments.size(); i++) {
        auto blockArgument = blockInputArguments[i]->getIdentifierRef();
        auto blockTargetArgument = blockTargetArguments[i]->getIdentifierRef();
        auto frameFlounderValue = frame.getValue(blockArgument);
        if (frame.contains(blockTargetArgument)) {
            auto targetFrameFlounderValue = frame.getValue(blockTargetArgument);
            MIR_append_insn(ctx,
                            func,
                            MIR_new_insn(ctx,
                                         MIR_MOV,
                                         MIR_new_reg_op(ctx, targetFrameFlounderValue),
                                         MIR_new_reg_op(ctx, frameFlounderValue)));
            frame.setValue(blockTargetArgument, frameFlounderValue);
        } else {
            auto targetVreg = getReg(blockTargetArgument, blockTargetArguments[i]->getStamp(), frame);
            MIR_append_insn(ctx,
                            func,
                            MIR_new_insn(ctx, MIR_MOV, MIR_new_reg_op(ctx, targetVreg), MIR_new_reg_op(ctx, frameFlounderValue)));
            frame.setValue(blockTargetArgument, targetVreg);
        }
    }
    return frame;
}

MIRLoweringProvider::MIRFrame
MIRLoweringProvider::LoweringContext::processInlineBlockInvocation(IR::Operations::BasicBlockInvocation& bi, MIRFrame& frame) {

    auto blockInputArguments = bi.getArguments();
    auto blockTargetArguments = bi.getBlock()->getArguments();
    auto inputArguments = std::set<std::string>();
    for (uint64_t i = 0; i < blockInputArguments.size(); i++) {
        inputArguments.emplace(blockInputArguments[i]->getIdentifierRef());
        frame.setValue(blockTargetArguments[i]->getIdentifierRef(), frame.getValue(blockInputArguments[i]->getIdentifierRef()));
    }
    return frame;
}
void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::CastOperation>& castOp,
                                                   MIRFrame& frame) {
    auto inputReg = frame.getValue(castOp->getInput()->getIdentifierRef());
    auto inputType = getType(castOp->getInput()->getStamp());
    auto resultType = getType(castOp->getInput()->getStamp());

    auto resultReg = getReg(castOp->getIdentifierRef(), castOp->getStamp(), frame);

    MIR_append_insn(ctx, func, MIR_new_insn(ctx, MIR_MOV, MIR_new_reg_op(ctx, resultReg), MIR_new_reg_op(ctx, inputReg)));

    frame.setValue(castOp->getIdentifierRef(), resultReg);
}
void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::AddOperation>& addOpt, MIRFrame& frame) {

    auto leftInput = frame.getValue(addOpt->getLeftInput()->getIdentifierRef());
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifierRef());
    auto resultReg = getReg(addOpt->getIdentifierRef(), addOpt->getStamp(), frame);

    MIR_insn_code_t inst;
    switch (getType(addOpt->getStamp())) {
        case MIR_T_I8:
        case MIR_T_U8:
        case MIR_T_I16:
        case MIR_T_U16:
        case MIR_T_I32:
        case MIR_T_U32:
        case MIR_T_I64:
        case MIR_T_U64: inst = MIR_ADD; break;
        case MIR_T_F: inst = MIR_FADD; break;
        case MIR_T_D: inst = MIR_DADD; break;
        case MIR_T_P: inst = MIR_ADD; break;
        default: NES_THROW_RUNTIME_ERROR("not valid type");
    }
    auto mirADD =
        MIR_new_insn(ctx, inst, MIR_new_reg_op(ctx, resultReg), MIR_new_reg_op(ctx, leftInput), MIR_new_reg_op(ctx, rightInput));
    MIR_append_insn(ctx, func, mirADD);
    frame.setValue(addOpt->getIdentifierRef(), resultReg);
}

void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::MulOperation>& mulOp, MIRFrame& frame) {
    auto leftInput = frame.getValue(mulOp->getLeftInput()->getIdentifierRef());
    auto rightInput = frame.getValue(mulOp->getRightInput()->getIdentifierRef());
    auto resultReg = getReg(mulOp->getIdentifierRef(), mulOp->getStamp(), frame);

    MIR_insn_code_t inst;
    switch (getType(mulOp->getStamp())) {
        case MIR_T_I8:
        case MIR_T_U8:
        case MIR_T_I16:
        case MIR_T_U16:
        case MIR_T_I32:
        case MIR_T_U32:
        case MIR_T_I64:
        case MIR_T_U64: inst = MIR_MUL; break;
        case MIR_T_F: inst = MIR_FMUL; break;
        case MIR_T_D: inst = MIR_DMUL; break;
        default: NES_THROW_RUNTIME_ERROR("not valid type");
    }
    auto mirMul =
        MIR_new_insn(ctx, inst, MIR_new_reg_op(ctx, resultReg), MIR_new_reg_op(ctx, leftInput), MIR_new_reg_op(ctx, rightInput));
    MIR_append_insn(ctx, func, mirMul);
    frame.setValue(mulOp->getIdentifierRef(), resultReg);
}

void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::SubOperation>& subOp, MIRFrame& frame) {
    auto leftInput = frame.getValue(subOp->getLeftInput()->getIdentifierRef());
    auto rightInput = frame.getValue(subOp->getRightInput()->getIdentifierRef());
    auto resultReg = getReg(subOp->getIdentifierRef(), subOp->getStamp(), frame);

    MIR_insn_code_t inst;
    switch (getType(subOp->getStamp())) {
        case MIR_T_I8:
        case MIR_T_U8:
        case MIR_T_I16:
        case MIR_T_U16:
        case MIR_T_I32:
        case MIR_T_U32:
        case MIR_T_I64:
        case MIR_T_U64: inst = MIR_SUB; break;
        case MIR_T_F: inst = MIR_FSUB; break;
        case MIR_T_D: inst = MIR_DSUB; break;
        default: NES_THROW_RUNTIME_ERROR("not valid type");
    }
    auto mirSub =
        MIR_new_insn(ctx, inst, MIR_new_reg_op(ctx, resultReg), MIR_new_reg_op(ctx, leftInput), MIR_new_reg_op(ctx, rightInput));
    MIR_append_insn(ctx, func, mirSub);
    frame.setValue(subOp->getIdentifierRef(), resultReg);
}

void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::LoadOperation>& opt, MIRFrame& frame) {
    auto address = frame.getValue(opt->getAddress()->getIdentifierRef());

    auto resultReg = getReg(opt->getIdentifierRef(), opt->getStamp(), frame);
    frame.setValue(opt->getIdentifierRef(), resultReg);
    auto type = getType(opt->getStamp());
    MIR_op_t memOp = MIR_new_mem_op(ctx, type, 0, address, 0, 1);
    MIR_append_insn(ctx, func, MIR_new_insn(ctx, MIR_MOV, MIR_new_reg_op(ctx, resultReg), memOp));

    /* auto memNode = program.mem(address);
    auto resultVreg = createVreg(opt->getIdentifier(), opt->getStamp(), frame);
    program << program.mov(resultVreg, memNode);*/
}

void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::StoreOperation>& opt, MIRFrame& frame) {
    auto address = frame.getValue(opt->getAddress()->getIdentifierRef());
    auto value = frame.getValue(opt->getValue()->getIdentifierRef());

    auto type = getType(opt->getValue()->getStamp());
    MIR_op_t memOp = MIR_new_mem_op(ctx, type, 0, address, 0, 1);
    MIR_append_insn(ctx, func, MIR_new_insn(ctx, MIR_MOV, memOp, MIR_new_reg_op(ctx, value)));
}

void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::BranchOperation>& branchOp,
                                                   MIRFrame& frame) {
    auto targetBlockName = branchOp->getNextBlockInvocation().getBlock()->getIdentifier();

    if (!labels.contains(targetBlockName)) {
        labels[targetBlockName] = MIR_new_label(ctx);
    }
    MIR_insn_t targetLabel = labels[targetBlockName];

    // prepate args
    auto targetFrame = processBlockInvocation(branchOp->getNextBlockInvocation(), frame);
    auto mirJumpTrue = MIR_new_insn(ctx, MIR_JMP, MIR_new_label_op(ctx, targetLabel));
    MIR_append_insn(ctx, func, mirJumpTrue);

    process(branchOp->getNextBlockInvocation().getBlock(), frame);
}

void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::IfOperation>& ifOpt, MIRFrame& frame) {

    auto inputValue = frame.getValue(ifOpt->getValue()->getIdentifierRef());
    auto trueBlockName = ifOpt->getTrueBlockInvocation().getBlock()->getIdentifier();
    auto falseBlockName = ifOpt->getFalseBlockInvocation().getBlock()->getIdentifier();

    if (!labels.contains(trueBlockName)) {
        labels[trueBlockName] = MIR_new_label(ctx);
    }
    if (!labels.contains(falseBlockName)) {
        labels[falseBlockName] = MIR_new_label(ctx);
    }
    MIR_insn_t trueLabel = labels[trueBlockName];
    MIR_insn_t falseLabel = labels[falseBlockName];

    // prepare args for true
    processBlockInvocation(ifOpt->getTrueBlockInvocation(), frame);
    auto mirJumpTrue = MIR_new_insn(ctx, MIR_BT, MIR_new_label_op(ctx, trueLabel), MIR_new_reg_op(ctx, inputValue));
    MIR_append_insn(ctx, func, mirJumpTrue);

    // prepare args for false
    processBlockInvocation(ifOpt->getFalseBlockInvocation(), frame);
    auto mirJumpFalse = MIR_new_insn(ctx, MIR_JMP, MIR_new_label_op(ctx, falseLabel));
    MIR_append_insn(ctx, func, mirJumpFalse);

    process(ifOpt->getTrueBlockInvocation().getBlock(), frame);
    process(ifOpt->getFalseBlockInvocation().getBlock(), frame);
}

void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::ProxyCallOperation>& opt,
                                                   MIRFrame& frame) {

    /*
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
*/

    if (!functions.contains(opt->getFunctionSymbol())) {
        Functions function;
        function.name = opt->getFunctionSymbol();
        function.protoName = opt->getFunctionSymbol() + "_proto";
        MIR_load_external(ctx, function.name.data(), opt->getFunctionPtr());
        function.address = MIR_new_import(ctx, function.name.data());

        auto arguments = opt->getInputArguments();
        MIR_var_t* args = (MIR_var_t*) alloca(sizeof(MIR_var_t) * arguments.size());
        for (auto i = 0ull; i < arguments.size(); i++) {
            auto& argument = arguments[i];
            args[i].type = getType(argument->getStamp());
            args[i].name = argument->getIdentifierRef().data();
        }

        if (!opt->getStamp()->isVoid()) {
            auto resType = getType(opt->getStamp());
            function.proto = MIR_new_vararg_proto_arr(ctx, function.protoName.data(), 1, &resType, arguments.size(), args);
        } else {
            function.proto = MIR_new_vararg_proto_arr(ctx, function.protoName.data(), 0, nullptr, arguments.size(), args);
        }
        functions[opt->getFunctionSymbol()] = function;
    }

    auto& function = functions[opt->getFunctionSymbol()];

    MIR_op_t* mirArgs = (MIR_op_t*) alloca(sizeof(MIR_op_t) * (20));
    mirArgs[0] = MIR_new_ref_op(ctx, function.proto);
    mirArgs[1] = MIR_new_ref_op(ctx, function.address);
    int i = 2;
    if (!opt->getStamp()->isVoid()) {
        auto resultReg = getReg(opt->getIdentifierRef(), opt->getStamp(), frame);
        frame.setValue(opt->getIdentifierRef(), resultReg);
        mirArgs[i] = MIR_new_reg_op(ctx, resultReg);
        i++;
    }
    auto arguments = opt->getInputArguments();
    for (auto& arg : arguments) {
        auto value = frame.getValue(arg->getIdentifierRef());
        mirArgs[i] = MIR_new_reg_op(ctx, value);
        i++;
    }

    auto call = MIR_new_insn_arr(ctx, MIR_CALL, i, mirArgs);
    /*
        auto call = MIR_new_call_insn(ctx,
                                      5,
                                      MIR_new_ref_op(ctx, protoF),
                                      MIR_new_ref_op(ctx, importF),
                                      MIR_new_reg_op(ctx, resultReg),
                                      MIR_new_reg_op(ctx, callArguments[0]),
                                      MIR_new_reg_op(ctx, callArguments[1]));
*/
    MIR_append_insn(ctx, func, call);
}

void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::LoopOperation>& opt, MIRFrame& frame) {
    auto loopHead = opt->getLoopHeadBlock();
    auto targetFrame = processInlineBlockInvocation(loopHead, frame);
    processInline(loopHead.getBlock(), targetFrame);
}

MIR_type_t MIRLoweringProvider::LoweringContext::getType(const IR::Types::StampPtr& stamp) {
    if (stamp->isInteger()) {
        auto value = cast<IR::Types::IntegerStamp>(stamp);
        if (value->isSigned()) {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::BitWidth::I8: return MIR_T_I8;
                case IR::Types::IntegerStamp::BitWidth::I16: return MIR_T_I16;
                case IR::Types::IntegerStamp::BitWidth::I32: return MIR_T_I32;
                case IR::Types::IntegerStamp::BitWidth::I64: return MIR_T_I64;
            }
        } else {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::BitWidth::I8: return MIR_T_U8;
                case IR::Types::IntegerStamp::BitWidth::I16: return MIR_T_U16;
                case IR::Types::IntegerStamp::BitWidth::I32: return MIR_T_U32;
                case IR::Types::IntegerStamp::BitWidth::I64: return MIR_T_U64;
            }
        }
    } else if (stamp->isFloat()) {
        auto value = cast<IR::Types::FloatStamp>(stamp);
        switch (value->getBitWidth()) {
            case IR::Types::FloatStamp::BitWidth::F32: return MIR_T_F;
            case IR::Types::FloatStamp::BitWidth::F64: return MIR_T_D;
        }
    } else if (stamp->isAddress()) {
        return MIR_T_P;
    } else if (stamp->isBoolean()) {
        return MIR_T_I64;
    }
    NES_NOT_IMPLEMENTED();
}

void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::AndOperation>& opt,
                                                   NES::Nautilus::Backends::MIR::MIRLoweringProvider::MIRFrame& frame) {
    auto leftInput = frame.getValue(opt->getLeftInput()->getIdentifierRef());
    auto rightInput = frame.getValue(opt->getRightInput()->getIdentifierRef());
    auto resultVar = getReg(opt->getIdentifierRef(), opt->getStamp(), frame);
    frame.setValue(opt->getIdentifierRef(), resultVar);
    MIR_append_insn(ctx,
                    func,
                    MIR_new_insn(ctx,
                                 MIR_AND,
                                 MIR_new_reg_op(ctx, resultVar),
                                 MIR_new_reg_op(ctx, leftInput),
                                 MIR_new_reg_op(ctx, rightInput)));
}

void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::OrOperation>& opt,
                                                   NES::Nautilus::Backends::MIR::MIRLoweringProvider::MIRFrame& frame) {
    auto leftInput = frame.getValue(opt->getLeftInput()->getIdentifierRef());
    auto rightInput = frame.getValue(opt->getRightInput()->getIdentifierRef());
    auto resultVar = getReg(opt->getIdentifierRef(), opt->getStamp(), frame);
    frame.setValue(opt->getIdentifierRef(), resultVar);
    MIR_append_insn(ctx,
                    func,
                    MIR_new_insn(ctx,
                                 MIR_OR,
                                 MIR_new_reg_op(ctx, resultVar),
                                 MIR_new_reg_op(ctx, leftInput),
                                 MIR_new_reg_op(ctx, rightInput)));
}

void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::CompareOperation>& cmpOp,
                                                   MIRFrame& frame) {
    auto leftInput = frame.getValue(cmpOp->getLeftInput()->getIdentifierRef());
    auto rightInput = frame.getValue(cmpOp->getRightInput()->getIdentifierRef());
    auto resultVar = getReg(cmpOp->getIdentifierRef(), cmpOp->getStamp(), frame);
    frame.setValue(cmpOp->getIdentifierRef(), resultVar);

    MIR_insn_code_t comperator;
    if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::EQ) {
        comperator = MIR_EQ;
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::LT) {
        comperator = MIR_LT;
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::GT) {
        comperator = MIR_GT;
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::GE) {
        comperator = MIR_GE;
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::LE) {
        comperator = MIR_LE;
    } else {
        NES_NOT_IMPLEMENTED();
    }

    MIR_append_insn(ctx,
                    func,
                    MIR_new_insn(ctx,
                                 comperator,
                                 MIR_new_reg_op(ctx, resultVar),
                                 MIR_new_reg_op(ctx, leftInput),
                                 MIR_new_reg_op(ctx, rightInput)));
}

MIR_reg_t MIRLoweringProvider::LoweringContext::getReg(const std::string& identifier,
                                                       const std::shared_ptr<IR::Types::Stamp>& stamp,
                                                       MIRFrame& frame) {
    if (frame.contains(identifier))
        return frame.getValue(identifier);
    MIR_type_t regType;
    switch (getType(stamp)) {
        case MIR_T_I8:;
        case MIR_T_U8:;
        case MIR_T_I16:;
        case MIR_T_U16:;
        case MIR_T_I32:;
        case MIR_T_U32:;
        case MIR_T_I64:;
        case MIR_T_U64:;
        case MIR_T_P: regType = MIR_T_I64; break;
        case MIR_T_F: regType = MIR_T_F; break;
        case MIR_T_D: regType = MIR_T_D; break;
        default: NES_THROW_RUNTIME_ERROR("wrong reg type");
    }
    auto reg = MIR_new_func_reg(ctx, func->u.func, regType, identifier.c_str());
    return reg;
}

void MIRLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::Operation>& opt, MIRFrame& frame) {
    switch (opt->getOperationType()) {
        case IR::Operations::Operation::OperationType::ConstBooleanOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(opt);
            auto constMIRInt = MIR_new_int_op(ctx, constInt->getValue());
            auto resultReg = getReg(constInt->getIdentifierRef(), constInt->getStamp(), frame);
            frame.setValue(constInt->getIdentifierRef(), resultReg);
            MIR_append_insn(ctx, func, MIR_new_insn(ctx, MIR_MOV, MIR_new_reg_op(ctx, resultReg), constMIRInt));
            return;
        }
        case IR::Operations::Operation::OperationType::ConstIntOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstIntOperation>(opt);
            auto constMIRInt = MIR_new_int_op(ctx, constInt->getValue());
            auto resultReg = getReg(constInt->getIdentifierRef(), constInt->getStamp(), frame);
            frame.setValue(constInt->getIdentifierRef(), resultReg);
            MIR_append_insn(ctx, func, MIR_new_insn(ctx, MIR_MOV, MIR_new_reg_op(ctx, resultReg), constMIRInt));
            return;
        }
        case IR::Operations::Operation::OperationType::ConstFloatOp: {
            auto constFloat = std::static_pointer_cast<IR::Operations::ConstFloatOperation>(opt);
            auto type = getType(constFloat->getStamp());
            auto constMIR = (type == MIR_T_F) ? MIR_new_float_op(ctx, constFloat->getValue())
                                              : MIR_new_double_op(ctx, constFloat->getValue());
            auto moveMir = (type == MIR_T_F) ? MIR_FMOV : MIR_DMOV;
            auto resultReg = getReg(constFloat->getIdentifierRef(), constFloat->getStamp(), frame);
            frame.setValue(constFloat->getIdentifierRef(), resultReg);
            MIR_append_insn(ctx, func, MIR_new_insn(ctx, moveMir, MIR_new_reg_op(ctx, resultReg), constMIR));
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
                MIR_append_insn(ctx, func, MIR_new_ret_insn(ctx, 1, MIR_new_reg_op(ctx, returnFOp)));
            } else {
                MIR_append_insn(ctx, func, MIR_new_ret_insn(ctx, 0));
            }
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
        case IR::Operations::Operation::OperationType::NegateOp: {
            auto neg = std::static_pointer_cast<IR::Operations::NegateOperation>(opt);
            auto input = frame.getValue(neg->getInput()->getIdentifierRef());
            auto resultVar = getReg(opt->getIdentifierRef(), opt->getStamp(), frame);
            frame.setValue(opt->getIdentifierRef(), resultVar);
            MIR_append_insn(ctx,
                            func,
                            MIR_new_insn(ctx,
                                         MIR_XOR,
                                         MIR_new_reg_op(ctx, resultVar),
                                         MIR_new_reg_op(ctx, input),
                                         MIR_new_int_op(ctx, 1)));
            return;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("Operation " << opt->toString() << " not handled");
        }
    }
}

}// namespace NES::Nautilus::Backends::MIR