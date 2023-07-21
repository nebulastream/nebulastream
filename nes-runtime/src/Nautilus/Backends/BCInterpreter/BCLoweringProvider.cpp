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

#include <Nautilus/Backends/BCInterpreter/BCLoweringProvider.hpp>
#include <Nautilus/Backends/BCInterpreter/ByteCode.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Types/AddressStamp.hpp>
#include <Nautilus/IR/Types/FloatStamp.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Nautilus::Backends::BC {

BCLoweringProvider::BCLoweringProvider() {}
BCLoweringProvider::LoweringContext::LoweringContext(std::shared_ptr<IR::IRGraph> ir) : ir(std::move(ir)) {}

std::tuple<Code, RegisterFile> BCLoweringProvider::lower(std::shared_ptr<IR::IRGraph> ir) {
    auto ctx = LoweringContext(ir);
    return ctx.process();
}

short BCLoweringProvider::RegisterProvider::allocRegister() {
    NES_ASSERT(currentRegister <= REGISTERS, "allocated to many registers.");
    return currentRegister++;
}

void BCLoweringProvider::RegisterProvider::freeRegister() {
    // TODO
}

std::tuple<Code, RegisterFile> BCLoweringProvider::LoweringContext::process() {
    defaultRegisterFile.fill(0);
    auto functionOperation = ir->getRootOperation();
    RegisterFrame rootFrame;
    auto functionBasicBlock = functionOperation->getFunctionBasicBlock();
    for (auto i = 0ull; i < functionBasicBlock->getArguments().size(); i++) {
        auto argument = functionBasicBlock->getArguments()[i];
        auto argumentRegister = registerProvider.allocRegister();
        rootFrame.setValue(argument->getIdentifier(), argumentRegister);
        program.arguments.emplace_back(argumentRegister);
    }
    this->process(functionBasicBlock, rootFrame);
    NES_INFO("Allocated Registers: {}", this->registerProvider.allocRegister());
    return std::make_tuple(program, defaultRegisterFile);
}

short BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::BasicBlock>& block, RegisterFrame& frame) {
    // assume that all argument registers are correctly set
    auto entry = activeBlocks.find(block->getIdentifier());
    if (entry == activeBlocks.end()) {
        short blockIndex = program.blocks.size();
        activeBlocks.emplace(block->getIdentifier(), blockIndex);
        // create bytecode block;
        program.blocks.emplace_back();
        for (auto& opt : block->getOperations()) {
            this->process(opt, blockIndex, frame);
        }
        return blockIndex;
    } else {
        return entry->second;
    }
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::AndOperation>& addOpt,
                                                  short block,
                                                  RegisterFrame& frame) {
    auto leftInput = frame.getValue(addOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());
    auto resultReg = getResultRegister(addOpt, frame);
    frame.setValue(addOpt->getIdentifier(), resultReg);
    ByteCode bc = ByteCode::AND_b;
    OpCode oc = {bc, leftInput, rightInput, resultReg};
    program.blocks[block].code.emplace_back(oc);
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::OrOperation>& addOpt,
                                                  short block,
                                                  RegisterFrame& frame) {
    auto leftInput = frame.getValue(addOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());
    auto resultReg = getResultRegister(addOpt, frame);
    frame.setValue(addOpt->getIdentifier(), resultReg);
    ByteCode bc = ByteCode::OR_b;
    OpCode oc = {bc, leftInput, rightInput, resultReg};
    program.blocks[block].code.emplace_back(oc);
}

Type getType(const IR::Types::StampPtr& stamp) {
    if (stamp->isInteger()) {
        auto value = cast<IR::Types::IntegerStamp>(stamp);
        if (value->isSigned()) {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::BitWidth::I8: return Type::i8;
                case IR::Types::IntegerStamp::BitWidth::I16: return Type::i16;
                case IR::Types::IntegerStamp::BitWidth::I32: return Type::i32;
                case IR::Types::IntegerStamp::BitWidth::I64: return Type::i64;
            }
        } else {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::BitWidth::I8: return Type::ui8;
                case IR::Types::IntegerStamp::BitWidth::I16: return Type::ui16;
                case IR::Types::IntegerStamp::BitWidth::I32: return Type::ui32;
                case IR::Types::IntegerStamp::BitWidth::I64: return Type::ui64;
            }
        }
    } else if (stamp->isFloat()) {
        auto value = cast<IR::Types::FloatStamp>(stamp);
        switch (value->getBitWidth()) {
            case IR::Types::FloatStamp::BitWidth::F32: return Type::f;
            case IR::Types::FloatStamp::BitWidth::F64: return Type::d;
        }
    } else if (stamp->isAddress()) {
        return Type::ptr;
    } else if (stamp->isVoid()) {
        return Type::v;
    } else if (stamp->isBoolean()) {
        return Type::b;
    }
    NES_NOT_IMPLEMENTED();
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::AddOperation>& addOpt,
                                                  short block,
                                                  RegisterFrame& frame) {
    auto leftInput = frame.getValue(addOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());
    auto resultReg = getResultRegister(addOpt, frame);
    frame.setValue(addOpt->getIdentifier(), resultReg);
    auto type = addOpt->getStamp();
    ByteCode bc;
    switch (getType(type)) {
        case Type::i8: bc = ByteCode::ADD_i8; break;
        case Type::i16: bc = ByteCode::ADD_i16; break;
        case Type::i32: bc = ByteCode::ADD_i32; break;
        case Type::i64: bc = ByteCode::ADD_i64; break;
        case Type::ui8: bc = ByteCode::ADD_ui8; break;
        case Type::ui16: bc = ByteCode::ADD_ui16; break;
        case Type::ui32: bc = ByteCode::ADD_ui32; break;
        case Type::ui64: bc = ByteCode::ADD_ui64; break;
        case Type::d: bc = ByteCode::ADD_d; break;
        case Type::f: bc = ByteCode::ADD_f; break;
        case Type::ptr: bc = ByteCode::ADD_i64; break;
        default: {
            NES_NOT_IMPLEMENTED();
        }
    }
    OpCode oc = {bc, leftInput, rightInput, resultReg};
    program.blocks[block].code.emplace_back(oc);
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::SubOperation>& subOpt,
                                                  short block,
                                                  RegisterFrame& frame) {
    auto leftInput = frame.getValue(subOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(subOpt->getRightInput()->getIdentifier());
    auto resultReg = getResultRegister(subOpt, frame);
    frame.setValue(subOpt->getIdentifier(), resultReg);
    ByteCode bc;
    switch (getType(subOpt->getStamp())) {
        case Type::i8: bc = ByteCode::SUB_i8; break;
        case Type::i16: bc = ByteCode::SUB_i16; break;
        case Type::i32: bc = ByteCode::SUB_i32; break;
        case Type::i64: bc = ByteCode::SUB_i64; break;
        case Type::ui8: bc = ByteCode::SUB_ui8; break;
        case Type::ui16: bc = ByteCode::SUB_ui16; break;
        case Type::ui32: bc = ByteCode::SUB_ui32; break;
        case Type::ui64: bc = ByteCode::SUB_ui64; break;
        case Type::d: bc = ByteCode::SUB_d; break;
        case Type::f: bc = ByteCode::SUB_f; break;
        case Type::ptr: bc = ByteCode::SUB_i64; break;
        default: {
            NES_NOT_IMPLEMENTED();
        }
    }
    OpCode oc = {bc, leftInput, rightInput, resultReg};
    program.blocks[block].code.emplace_back(oc);
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::MulOperation>& mulOpt,
                                                  short block,
                                                  RegisterFrame& frame) {
    auto leftInput = frame.getValue(mulOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(mulOpt->getRightInput()->getIdentifier());
    auto resultReg = getResultRegister(mulOpt, frame);
    frame.setValue(mulOpt->getIdentifier(), resultReg);

    ByteCode bc;
    switch (getType(mulOpt->getStamp())) {
        case Type::i8: bc = ByteCode::MUL_i8; break;
        case Type::i16: bc = ByteCode::MUL_i16; break;
        case Type::i32: bc = ByteCode::MUL_i32; break;
        case Type::i64: bc = ByteCode::MUL_i64; break;
        case Type::ui8: bc = ByteCode::MUL_ui8; break;
        case Type::ui16: bc = ByteCode::MUL_ui16; break;
        case Type::ui32: bc = ByteCode::MUL_ui32; break;
        case Type::ui64: bc = ByteCode::MUL_ui64; break;
        case Type::d: bc = ByteCode::MUL_d; break;
        case Type::f: bc = ByteCode::MUL_f; break;
        case Type::ptr: bc = ByteCode::MUL_i64; break;
        default: {
            NES_NOT_IMPLEMENTED();
        }
    }
    OpCode oc = {bc, leftInput, rightInput, resultReg};
    program.blocks[block].code.emplace_back(oc);
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::DivOperation>& divOp,
                                                  short block,
                                                  RegisterFrame& frame) {
    auto leftInput = frame.getValue(divOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(divOp->getRightInput()->getIdentifier());
    auto resultReg = getResultRegister(divOp, frame);
    frame.setValue(divOp->getIdentifier(), resultReg);
    ByteCode bc;
    switch (getType(divOp->getStamp())) {
        case Type::i8: bc = ByteCode::DIV_i8; break;
        case Type::i16: bc = ByteCode::DIV_i16; break;
        case Type::i32: bc = ByteCode::DIV_i32; break;
        case Type::i64: bc = ByteCode::DIV_i64; break;
        case Type::ui8: bc = ByteCode::DIV_ui8; break;
        case Type::ui16: bc = ByteCode::DIV_ui16; break;
        case Type::ui32: bc = ByteCode::DIV_ui32; break;
        case Type::ui64: bc = ByteCode::DIV_ui64; break;
        case Type::d: bc = ByteCode::DIV_d; break;
        case Type::f: bc = ByteCode::DIV_f; break;
        case Type::ptr: bc = ByteCode::DIV_i64; break;
        default: {
            NES_NOT_IMPLEMENTED();
        }
    }
    OpCode oc = {bc, leftInput, rightInput, resultReg};
    program.blocks[block].code.emplace_back(oc);
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::CompareOperation>& cmpOp,
                                                  short block,
                                                  RegisterFrame& frame) {
    auto leftInput = frame.getValue(cmpOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(cmpOp->getRightInput()->getIdentifier());
    auto resultReg = getResultRegister(cmpOp, frame);
    frame.setValue(cmpOp->getIdentifier(), resultReg);

    if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::EQ) {
        auto type = cmpOp->getLeftInput()->getStamp();
        ByteCode bc;
        switch (getType(type)) {
            case Type::i8: bc = ByteCode::EQ_i8; break;
            case Type::i16: bc = ByteCode::EQ_i16; break;
            case Type::i32: bc = ByteCode::EQ_i32; break;
            case Type::i64: bc = ByteCode::EQ_i64; break;
            case Type::ui8: bc = ByteCode::EQ_ui8; break;
            case Type::ui16: bc = ByteCode::EQ_ui16; break;
            case Type::ui32: bc = ByteCode::EQ_ui32; break;
            case Type::ui64: bc = ByteCode::EQ_ui64; break;
            case Type::d: bc = ByteCode::EQ_d; break;
            case Type::f: bc = ByteCode::EQ_f; break;
            case Type::ptr: bc = ByteCode::EQ_i64; break;
            default: {
                NES_NOT_IMPLEMENTED();
            }
        }
        OpCode oc = {bc, leftInput, rightInput, resultReg};
        program.blocks[block].code.emplace_back(oc);
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::LT) {
        auto type = cmpOp->getLeftInput()->getStamp();
        ByteCode bc;
        switch (getType(type)) {
            case Type::i8: bc = ByteCode::LESS_THAN_i8; break;
            case Type::i16: bc = ByteCode::LESS_THAN_i16; break;
            case Type::i32: bc = ByteCode::LESS_THAN_i32; break;
            case Type::i64: bc = ByteCode::LESS_THAN_i64; break;
            case Type::ui8: bc = ByteCode::LESS_THAN_ui8; break;
            case Type::ui16: bc = ByteCode::LESS_THAN_ui16; break;
            case Type::ui32: bc = ByteCode::LESS_THAN_ui32; break;
            case Type::ui64: bc = ByteCode::LESS_THAN_ui64; break;
            case Type::d: bc = ByteCode::LESS_THAN_d; break;
            case Type::f: bc = ByteCode::LESS_THAN_f; break;
            case Type::ptr: bc = ByteCode::LESS_THAN_i64; break;
            default: {
                NES_NOT_IMPLEMENTED();
            }
        }
        OpCode oc = {bc, leftInput, rightInput, resultReg};
        program.blocks[block].code.emplace_back(oc);
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::GT) {
        ByteCode bc;
        auto type = cmpOp->getLeftInput()->getStamp();
        switch (getType(type)) {
            case Type::i8: bc = ByteCode::GREATER_THAN_i8; break;
            case Type::i16: bc = ByteCode::GREATER_THAN_i16; break;
            case Type::i32: bc = ByteCode::GREATER_THAN_i32; break;
            case Type::i64: bc = ByteCode::GREATER_THAN_i64; break;
            case Type::ui8: bc = ByteCode::GREATER_THAN_ui8; break;
            case Type::ui16: bc = ByteCode::GREATER_THAN_ui16; break;
            case Type::ui32: bc = ByteCode::GREATER_THAN_ui32; break;
            case Type::ui64: bc = ByteCode::GREATER_THAN_ui64; break;
            case Type::d: bc = ByteCode::GREATER_THAN_d; break;
            case Type::f: bc = ByteCode::GREATER_THAN_f; break;
            case Type::ptr: bc = ByteCode::GREATER_THAN_i64; break;
            default: {
                NES_NOT_IMPLEMENTED();
            }
        }
        OpCode oc = {bc, leftInput, rightInput, resultReg};
        program.blocks[block].code.emplace_back(oc);
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::LT) {
        ByteCode bc;
        auto type = cmpOp->getLeftInput()->getStamp();
        switch (getType(type)) {
            case Type::f: bc = ByteCode::LESS_THAN_f; break;
            case Type::d: bc = ByteCode::LESS_THAN_d; break;
            default: {
                NES_NOT_IMPLEMENTED();
            }
        }
        OpCode oc = {bc, leftInput, rightInput, resultReg};
        program.blocks[block].code.emplace_back(oc);
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::EQ) {
        ByteCode bc;
        auto type = cmpOp->getLeftInput()->getStamp();
        switch (getType(type)) {
            case Type::f: bc = ByteCode::EQ_f; break;
            case Type::d: bc = ByteCode::EQ_d; break;
            default: {
                NES_NOT_IMPLEMENTED();
            }
        }
        OpCode oc = {bc, leftInput, rightInput, resultReg};
        program.blocks[block].code.emplace_back(oc);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::LoadOperation>& loadOp,
                                                  short block,
                                                  RegisterFrame& frame) {
    auto address = frame.getValue(loadOp->getAddress()->getIdentifier());
    auto resultReg = getResultRegister(loadOp, frame);
    frame.setValue(loadOp->getIdentifier(), resultReg);
    auto type = loadOp->getStamp();
    ByteCode bc;
    switch (getType(type)) {
        case Type::i8: bc = ByteCode::LOAD_i8; break;
        case Type::i16: bc = ByteCode::LOAD_i16; break;
        case Type::i32: bc = ByteCode::LOAD_i32; break;
        case Type::i64: bc = ByteCode::LOAD_i64; break;
        case Type::ui8: bc = ByteCode::LOAD_ui8; break;
        case Type::ui16: bc = ByteCode::LOAD_ui16; break;
        case Type::ui32: bc = ByteCode::LOAD_ui32; break;
        case Type::ui64: bc = ByteCode::LOAD_ui64; break;
        case Type::f: bc = ByteCode::LOAD_f; break;
        case Type::d: bc = ByteCode::LOAD_d; break;
        case Type::ptr: bc = ByteCode::LOAD_i64; break;
        case Type::b: bc = ByteCode::LOAD_b; break;
        default: {
            NES_NOT_IMPLEMENTED();
        }
    }

    OpCode oc = {bc, address, -1, resultReg};
    program.blocks[block].code.emplace_back(oc);
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::StoreOperation>& storeOp,
                                                  short block,
                                                  RegisterFrame& frame) {
    auto addressReg = frame.getValue(storeOp->getAddress()->getIdentifier());
    auto valueReg = frame.getValue(storeOp->getValue()->getIdentifier());
    auto type = storeOp->getValue()->getStamp();
    ByteCode bc;
    switch (getType(type)) {
        case Type::i8: bc = ByteCode::STORE_i8; break;
        case Type::i16: bc = ByteCode::STORE_i16; break;
        case Type::i32: bc = ByteCode::STORE_i32; break;
        case Type::i64: bc = ByteCode::STORE_i64; break;
        case Type::ui8: bc = ByteCode::STORE_ui8; break;
        case Type::ui16: bc = ByteCode::STORE_ui16; break;
        case Type::ui32: bc = ByteCode::STORE_ui32; break;
        case Type::ui64: bc = ByteCode::STORE_ui64; break;
        case Type::d: bc = ByteCode::STORE_d; break;
        case Type::f: bc = ByteCode::STORE_f; break;
        case Type::ptr: bc = ByteCode::STORE_i64; break;
        case Type::b: bc = ByteCode::STORE_b; break;
        default: {
            NES_NOT_IMPLEMENTED();
        }
    }
    OpCode oc = {bc, addressReg, valueReg, -1};
    program.blocks[block].code.emplace_back(oc);
}

void BCLoweringProvider::LoweringContext::process(IR::Operations::BasicBlockInvocation& bi,
                                                  short block,
                                                  RegisterFrame& parentFrame) {
    auto blockInputArguments = bi.getArguments();
    auto blockTargetArguments = bi.getBlock()->getArguments();
    for (uint64_t i = 0; i < blockInputArguments.size(); i++) {
        auto blockArgument = blockInputArguments[i]->getIdentifier();
        auto blockTargetArgument = blockTargetArguments[i]->getIdentifier();
        auto parentFrameReg = parentFrame.getValue(blockArgument);
        if (!parentFrame.contains(blockTargetArgument)) {
            //auto resultReg = registerProvider.allocRegister();
            // TODO use child frame
            parentFrame.setValue(blockTargetArgument, parentFrameReg);
            //OpCode oc = {ByteCode::REG_MOV, parentFrameReg, -1, resultReg};
            //program.blocks[block].code.emplace_back(oc);
        } else {
            // TODO use child frame
            auto resultReg = parentFrame.getValue(blockTargetArgument);
            if (resultReg != parentFrameReg) {
                OpCode oc = {ByteCode::REG_MOV, parentFrameReg, -1, resultReg};
                program.blocks[block].code.emplace_back(oc);
            }
        }
    }
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::IfOperation>& ifOpt,
                                                  short block,
                                                  RegisterFrame& frame) {
    auto conditionalReg = frame.getValue(ifOpt->getValue()->getIdentifier());
    process(ifOpt->getTrueBlockInvocation(), block, frame);
    process(ifOpt->getFalseBlockInvocation(), block, frame);
    auto trueBlockIndex = process(ifOpt->getTrueBlockInvocation().getBlock(), frame);
    auto falseBlockIndex = process(ifOpt->getFalseBlockInvocation().getBlock(), frame);
    program.blocks[block].terminatorOp = ConditionalJumpOp{conditionalReg, trueBlockIndex, falseBlockIndex};
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::BranchOperation>& branchOp,
                                                  short block,
                                                  RegisterFrame& frame) {
    process(branchOp->getNextBlockInvocation(), block, frame);
    auto blockIndex = process(branchOp->getNextBlockInvocation().getBlock(), frame);
    program.blocks[block].terminatorOp = BranchOp{blockIndex};
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::Operation>& opt,
                                                  short block,
                                                  RegisterFrame& frame) {
    switch (opt->getOperationType()) {
        case IR::Operations::Operation::OperationType::ConstBooleanOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(opt);
            auto defaultRegister = registerProvider.allocRegister();
            defaultRegisterFile[defaultRegister] = constInt->getValue();

            auto targetRegister = registerProvider.allocRegister();
            frame.setValue(constInt->getIdentifier(), targetRegister);
            OpCode oc = {ByteCode::REG_MOV, defaultRegister, -1, targetRegister};
            program.blocks[block].code.emplace_back(oc);
            return;
        }
        case IR::Operations::Operation::OperationType::ConstIntOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstIntOperation>(opt);
            auto defaultRegister = registerProvider.allocRegister();
            defaultRegisterFile[defaultRegister] = constInt->getValue();
            auto targetRegister = registerProvider.allocRegister();
            frame.setValue(constInt->getIdentifier(), targetRegister);
            OpCode oc = {ByteCode::REG_MOV, defaultRegister, -1, targetRegister};
            program.blocks[block].code.emplace_back(oc);
            return;
        }
        case IR::Operations::Operation::OperationType::ConstFloatOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstFloatOperation>(opt);
            auto defaultRegister = registerProvider.allocRegister();
            if (cast<IR::Types::FloatStamp>(constInt->getStamp())->getBitWidth() == IR::Types::FloatStamp::BitWidth::F32) {
                auto floatValue = (float) constInt->getValue();
                auto floatReg = reinterpret_cast<float*>(&defaultRegisterFile[defaultRegister]);
                *floatReg = floatValue;
            } else {
                auto floatValue = (double) constInt->getValue();
                auto floatReg = reinterpret_cast<double*>(&defaultRegisterFile[defaultRegister]);
                *floatReg = floatValue;
            }

            auto targetRegister = registerProvider.allocRegister();
            frame.setValue(constInt->getIdentifier(), targetRegister);
            OpCode oc = {ByteCode::REG_MOV, defaultRegister, -1, targetRegister};
            program.blocks[block].code.emplace_back(oc);
            return;
        }
        case IR::Operations::Operation::OperationType::AddOp: {
            auto addOpt = std::static_pointer_cast<IR::Operations::AddOperation>(opt);
            process(addOpt, block, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::MulOp: {
            auto mulOpt = std::static_pointer_cast<IR::Operations::MulOperation>(opt);
            process(mulOpt, block, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::SubOp: {
            auto subOpt = std::static_pointer_cast<IR::Operations::SubOperation>(opt);
            process(subOpt, block, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::DivOp: {
            auto divOpt = std::static_pointer_cast<IR::Operations::DivOperation>(opt);
            process(divOpt, block, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::ReturnOp: {
            auto returnOpt = std::static_pointer_cast<IR::Operations::ReturnOperation>(opt);
            if (returnOpt->hasReturnValue()) {
                auto returnFOp = frame.getValue(returnOpt->getReturnValue()->getIdentifier());
                program.blocks[block].terminatorOp = ReturnOp{returnFOp};
                program.returnType = getType(returnOpt->getReturnValue()->getStamp());
                return;
            } else {
                program.blocks[block].terminatorOp = ReturnOp{-1};
                return;
            }
        }
        case IR::Operations::Operation::OperationType::CompareOp: {
            auto compOpt = std::static_pointer_cast<IR::Operations::CompareOperation>(opt);
            process(compOpt, block, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::IfOp: {
            auto ifOpt = std::static_pointer_cast<IR::Operations::IfOperation>(opt);
            process(ifOpt, block, frame);
            return;
        }

        case IR::Operations::Operation::OperationType::BranchOp: {
            auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(opt);
            process(branchOp, block, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::LoadOp: {
            auto load = std::static_pointer_cast<IR::Operations::LoadOperation>(opt);
            process(load, block, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::StoreOp: {
            auto store = std::static_pointer_cast<IR::Operations::StoreOperation>(opt);
            process(store, block, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::ProxyCallOp: {
            auto call = std::static_pointer_cast<IR::Operations::ProxyCallOperation>(opt);
            process(call, block, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::OrOp: {
            auto call = std::static_pointer_cast<IR::Operations::OrOperation>(opt);
            process(call, block, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::AndOp: {
            auto call = std::static_pointer_cast<IR::Operations::AndOperation>(opt);
            process(call, block, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::NegateOp: {
            auto call = std::static_pointer_cast<IR::Operations::NegateOperation>(opt);
            process(call, block, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::CastOp: {
            auto cast = std::static_pointer_cast<IR::Operations::CastOperation>(opt);
            process(cast, block, frame);
            return;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("Operation " << opt->toString() << " not handled");
            return;
        }
    }
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::ProxyCallOperation>& opt,
                                                  short block,
                                                  RegisterFrame& frame) {
    // first try to lower to a native call if the correct stub was registered.
    auto returnValue = processNativeCall(opt, block, frame);
    if (!returnValue) {
        // if it was not possible to lower the call, we try to create a dynamic call using dyncall.h
        processDynamicCall(opt, block, frame);
    }
}

void BCLoweringProvider::LoweringContext::processDynamicCall(const std::shared_ptr<IR::Operations::ProxyCallOperation>& opt,
                                                             short block,
                                                             RegisterFrame& frame) {
    auto& code = program.blocks[block].code;
    NES_DEBUG("CREATE {}: {}", opt->toString(), opt->getStamp()->toString())
    auto arguments = opt->getInputArguments();

    // 1. reset dyncall stack
    code.emplace_back(ByteCode::DYNCALL_reset, -1, -1, -1);

    // 2. set dyncall arguments
    for (auto& arg : arguments) {
        auto argType = getType(arg->getStamp());
        ByteCode bc;
        switch (argType) {
            case Type::i8: bc = ByteCode::DYNCALL_arg_i8; break;
            case Type::i16: bc = ByteCode::DYNCALL_arg_i16; break;
            case Type::i32: bc = ByteCode::DYNCALL_arg_i32; break;
            case Type::i64: bc = ByteCode::DYNCALL_arg_i64; break;
            case Type::ui8: bc = ByteCode::DYNCALL_arg_i8; break;
            case Type::ui16: bc = ByteCode::DYNCALL_arg_i16; break;
            case Type::ui32: bc = ByteCode::DYNCALL_arg_i32; break;
            case Type::ui64: bc = ByteCode::DYNCALL_arg_i64; break;
            case Type::d: bc = ByteCode::DYNCALL_arg_d; break;
            case Type::f: bc = ByteCode::DYNCALL_arg_f; break;
            case Type::b: bc = ByteCode::DYNCALL_arg_b; break;
            case Type::ptr: bc = ByteCode::DYNCALL_arg_ptr; break;
            default: NES_THROW_RUNTIME_ERROR("Type not implemented");
        }
        auto registerSlot = frame.getValue(arg->getIdentifier());
        code.emplace_back(bc, registerSlot, -1, -1);
    }

    // 3. call function
    auto returnType = getType(opt->getStamp());
    ByteCode bc;
    switch (returnType) {
        case Type::i8: bc = ByteCode::DYNCALL_call_i8; break;
        case Type::i16: bc = ByteCode::DYNCALL_call_i16; break;
        case Type::i32: bc = ByteCode::DYNCALL_call_i32; break;
        case Type::i64: bc = ByteCode::DYNCALL_call_i64; break;
        case Type::ui8: bc = ByteCode::DYNCALL_call_i8; break;
        case Type::ui16: bc = ByteCode::DYNCALL_call_i16; break;
        case Type::ui32: bc = ByteCode::DYNCALL_call_i32; break;
        case Type::ui64: bc = ByteCode::DYNCALL_call_i64; break;
        case Type::ptr: bc = ByteCode::DYNCALL_call_ptr; break;
        case Type::v: bc = ByteCode::DYNCALL_call_v; break;
        case Type::b: bc = ByteCode::DYNCALL_call_b; break;
        default: NES_THROW_RUNTIME_ERROR("Type not implemented");
    }

    auto funcInfoRegister = registerProvider.allocRegister();
    defaultRegisterFile[funcInfoRegister] = (int64_t) opt->getFunctionPtr();

    if (!opt->getStamp()->isVoid()) {
        auto resultRegister = getResultRegister(opt, frame);
        frame.setValue(opt->getIdentifier(), resultRegister);
        code.emplace_back(bc, funcInfoRegister, -1, resultRegister);
    } else {
        code.emplace_back(bc, funcInfoRegister, -1, -1);
    }
}

bool BCLoweringProvider::LoweringContext::processNativeCall(const std::shared_ptr<IR::Operations::ProxyCallOperation>& opt,
                                                            short block,
                                                            RegisterFrame& frame) {

    // TODO the following code is very bad and manually checks function signatures. Type to come up with something more generic.
    NES_DEBUG("CREATE {}: {}", opt->toString(), opt->getStamp()->toString())
    auto arguments = opt->getInputArguments();
    ByteCode bc;
    if (opt->getStamp()->isVoid()) {
        if (arguments.empty()) {
            bc = ByteCode::CALL_v;
        } else if (arguments.size() == 1) {
            if (getType(arguments[0]->getStamp()) == Type::ptr) {
                bc = ByteCode::CALL_v_ptr;
            } else {
                return false;
            }
        } else if (arguments.size() == 2) {
            if (getType(arguments[0]->getStamp()) == Type::ptr && getType(arguments[1]->getStamp()) == Type::ui64) {
                bc = ByteCode::CALL_v_ptr_ui64;
            } else {
                return false;
            }
        } else if (arguments.size() == 3) {
            if (getType(arguments[0]->getStamp()) == Type::ptr && getType(arguments[1]->getStamp()) == Type::ptr
                && getType(arguments[2]->getStamp()) == Type::ptr) {
                bc = ByteCode::CALL_v_ptr_ptr_ptr;
            } else {
                return false;
            }
        } else if (arguments.size() == 7) {
            if (getType(arguments[0]->getStamp()) == Type::ptr && getType(arguments[1]->getStamp()) == Type::ptr
                && getType(arguments[2]->getStamp()) == Type::ptr && getType(arguments[3]->getStamp()) == Type::ui64
                && getType(arguments[4]->getStamp()) == Type::ui64 && getType(arguments[5]->getStamp()) == Type::ui64
                && getType(arguments[6]->getStamp()) == Type::ui64) {
                bc = ByteCode::CALL_v_ptr_ptr_ptr_ui64_ui64_ui64_ui64;
            } else {
                return false;
            }
        } else {
            // TODO support void function
            NES_NOT_IMPLEMENTED();
        }
    } else if (getType(opt->getStamp()) == Type::i64) {
        if (arguments.empty()) {
            bc = ByteCode::CALL_i64;
        } else if (arguments.size() == 1) {
            if (getType(arguments[0]->getStamp()) == Type::i64) {
                bc = ByteCode::CALL_i64_i64;
            } else if (getType(arguments[0]->getStamp()) == Type::ptr) {
                return false;
            } else {
                return false;
            }
        } else if (arguments.size() == 2) {
            if (getType(arguments[0]->getStamp()) == Type::i64 && getType(arguments[1]->getStamp()) == Type::i64) {
                bc = ByteCode::CALL_i64_i64_i64;
            } else {
                return false;
            }
        } else {
            return false;
        }
    } else if (getType(opt->getStamp()) == Type::ui64) {
        if (arguments.empty()) {
            return false;
        } else if (arguments.size() == 1) {
            if (getType(arguments[0]->getStamp()) == Type::i64) {
                return false;
            } else if (getType(arguments[0]->getStamp()) == Type::ptr) {
                bc = ByteCode::CALL_ui64_ptr;
            } else {
                return false;
            }
        } else if (arguments.size() == 2) {
            if (getType(arguments[0]->getStamp()) == Type::i64 && getType(arguments[1]->getStamp()) == Type::i64) {
                return false;
            } else if (getType(arguments[0]->getStamp()) == Type::ui64 && getType(arguments[1]->getStamp()) == Type::i64) {
                bc = ByteCode::CALL_ui64_ui64_i64;
            } else if (getType(arguments[0]->getStamp()) == Type::ui64 && getType(arguments[1]->getStamp()) == Type::i32) {
                bc = ByteCode::CALL_ui64_ui64_i32;
            } else if (getType(arguments[0]->getStamp()) == Type::ui64 && getType(arguments[1]->getStamp()) == Type::i8) {
                bc = ByteCode::CALL_ui64_ui64_i8;
            } else {
                return false;
            }
        } else {
            return false;
        }
    } else if (getType(opt->getStamp()) == Type::ptr) {
        if (arguments.empty()) {
            return false;
        } else if (arguments.size() == 1) {
            if (getType(arguments[0]->getStamp()) == Type::i64) {
                return false;
            } else if (getType(arguments[0]->getStamp()) == Type::ptr) {
                bc = ByteCode::CALL_ptr_ptr;
            } else {
                return false;
            }
        } else if (arguments.size() == 2) {
            if (getType(arguments[0]->getStamp()) == Type::i64 && getType(arguments[1]->getStamp()) == Type::i64) {
                return false;
            } else if (getType(arguments[0]->getStamp()) == Type::ptr && getType(arguments[1]->getStamp()) == Type::ui64) {
                bc = ByteCode::CALL_ptr_ptr_ui64;
            } else if (getType(arguments[0]->getStamp()) == Type::ptr && getType(arguments[1]->getStamp()) == Type::i64) {
                bc = ByteCode::CALL_ptr_ptr_i64;
            } else if (getType(arguments[0]->getStamp()) == Type::ptr && getType(arguments[1]->getStamp()) == Type::ptr) {
                bc = ByteCode::CALL_ptr_ptr_ptr;
            } else {
                return false;
            }
        } else {
            return false;
        }
    } else {
        return false;
    }

    std::vector<std::pair<short, Backends::BC::Type>> argRegisters;
    for (const auto& arg : arguments) {
        auto argReg = frame.getValue(arg->getIdentifier());
        argRegisters.emplace_back(argReg, Backends::BC::Type::i64);
    }

    auto fcall = new Backends::BC::FunctionCallTarget(argRegisters, opt->getFunctionPtr());
    auto funcInfoRegister = registerProvider.allocRegister();
    defaultRegisterFile[funcInfoRegister] = (int64_t) fcall;
    short resultRegister = -1;
    if (!opt->getStamp()->isVoid()) {
        resultRegister = getResultRegister(opt, frame);
        frame.setValue(opt->getIdentifier(), resultRegister);
    }
    OpCode oc = {bc, funcInfoRegister, -1, resultRegister};
    program.blocks[block].code.emplace_back(oc);
    return true;
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::NegateOperation>& negateOperation,
                                                  short block,
                                                  RegisterFrame& frame) {
    auto input = frame.getValue(negateOperation->getInput()->getIdentifier());
    auto resultReg = getResultRegister(negateOperation, frame);
    frame.setValue(negateOperation->getIdentifier(), resultReg);
    ByteCode bc = ByteCode::NOT_b;
    OpCode oc = {bc, input, -1, resultReg};
    program.blocks[block].code.emplace_back(oc);
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::CastOperation>& castOp,
                                                  short block,
                                                  RegisterFrame& frame) {
    auto input = frame.getValue(castOp->getInput()->getIdentifier());
    auto resultReg = getResultRegister(castOp, frame);
    frame.setValue(castOp->getIdentifier(), resultReg);
    auto srcType = getType(castOp->getInput()->getStamp());
    auto targetType = getType(castOp->getStamp());
    ByteCode bc;
    if (srcType == Type::i8) {
        switch (targetType) {
            case Type::i16: bc = ByteCode::CAST_i8_i16; break;
            case Type::i32: bc = ByteCode::CAST_i8_i32; break;
            case Type::i64: bc = ByteCode::CAST_i8_i64; break;
            case Type::ui8: bc = ByteCode::CAST_i8_ui8; break;
            case Type::ui16: bc = ByteCode::CAST_i8_ui16; break;
            case Type::ui32: bc = ByteCode::CAST_i8_ui32; break;
            case Type::ui64: bc = ByteCode::CAST_i8_ui64; break;
            case Type::f: bc = ByteCode::CAST_i8_f; break;
            case Type::d: bc = ByteCode::CAST_i8_d; break;
            default: NES_THROW_RUNTIME_ERROR("Cast from i8 not supported");
        }
    } else if (srcType == Type::i16) {
        switch (targetType) {
            case Type::i32: bc = ByteCode::CAST_i16_i32; break;
            case Type::i64: bc = ByteCode::CAST_i16_i64; break;
            case Type::ui16: bc = ByteCode::CAST_i16_ui16; break;
            case Type::ui32: bc = ByteCode::CAST_i16_ui32; break;
            case Type::ui64: bc = ByteCode::CAST_i16_ui64; break;
            case Type::f: bc = ByteCode::CAST_i16_f; break;
            case Type::d: bc = ByteCode::CAST_i16_d; break;
            default: NES_THROW_RUNTIME_ERROR("Cast from i16 not supported");
        }
    } else if (srcType == Type::i32) {
        switch (targetType) {
            case Type::i64: bc = ByteCode::CAST_i32_i64; break;
            case Type::ui32: bc = ByteCode::CAST_i32_ui32; break;
            case Type::ui64: bc = ByteCode::CAST_i32_ui64; break;
            case Type::f: bc = ByteCode::CAST_i32_f; break;
            case Type::d: bc = ByteCode::CAST_i32_d; break;
            default: NES_THROW_RUNTIME_ERROR("Cast from i32 not supported");
        }
    } else if (srcType == Type::i64) {
        switch (targetType) {
            case Type::ui64: bc = ByteCode::CAST_i64_ui64; break;
            case Type::f: bc = ByteCode::CAST_i64_f; break;
            case Type::d: bc = ByteCode::CAST_i64_d; break;
            default: NES_THROW_RUNTIME_ERROR("Cast from i64 not supported");
        }
    } else if (srcType == Type::ui8) {
        switch (targetType) {
            case Type::ui16: bc = ByteCode::CAST_ui8_ui16; break;
            case Type::ui32: bc = ByteCode::CAST_ui8_ui32; break;
            case Type::ui64: bc = ByteCode::CAST_ui8_ui64; break;
            case Type::i16: bc = ByteCode::CAST_ui8_i16; break;
            case Type::i32: bc = ByteCode::CAST_ui8_i32; break;
            case Type::i64: bc = ByteCode::CAST_ui8_i64; break;
            default: NES_THROW_RUNTIME_ERROR("Cast from ui8 not supported");
        }
    } else if (srcType == Type::ui16) {
        switch (targetType) {
            case Type::ui32: bc = ByteCode::CAST_ui16_ui32; break;
            case Type::ui64: bc = ByteCode::CAST_ui16_ui64; break;
            case Type::i32: bc = ByteCode::CAST_ui16_i32; break;
            case Type::i64: bc = ByteCode::CAST_ui16_i64; break;
            default: NES_THROW_RUNTIME_ERROR("Cast from ui16 not supported");
        }
    } else if (srcType == Type::ui32 && targetType == Type::ui64) {
        bc = ByteCode::CAST_ui32_ui64;
    } else if (srcType == Type::ui32 && targetType == Type::i64) {
        bc = ByteCode::CAST_ui32_i64;
    } else if (srcType == Type::f && targetType == Type::d) {
        bc = ByteCode::CAST_f_d;
    } else {
        NES_THROW_RUNTIME_ERROR("Cast from not supported");
    }
    OpCode oc = {bc, input, -1, resultReg};
    program.blocks[block].code.emplace_back(oc);
}

short BCLoweringProvider::LoweringContext::getResultRegister(const std::shared_ptr<IR::Operations::Operation>& opt,
                                                             RegisterFrame& frame) {
    auto optResultIdentifier = opt->getIdentifier();

    // if the result value of opt is directly passed to an block argument, then we can directly write the value to the correct target register.
    if (opt->getUsages().size() == 1) {
        auto* usage = opt->getUsages()[0];
        if (usage->getOperationType() == IR::Operations::Operation::OperationType::BlockInvocation) {
            auto bi = dynamic_cast<const IR::Operations::BasicBlockInvocation*>(usage);
            auto blockInputArguments = bi->getArguments();
            auto blockTargetArguments = bi->getBlock()->getArguments();

            std::vector<uint64_t> matchingArguments;

            for (uint64_t i = 0; i < blockInputArguments.size(); i++) {
                auto blockArgument = blockInputArguments[i]->getIdentifier();
                if (blockArgument == optResultIdentifier) {
                    matchingArguments.emplace_back(i);
                }
            }

            if (matchingArguments.size() == 1) {
                auto blockTargetArgumentIdentifier = blockTargetArguments[matchingArguments[0]]->getIdentifier();
                if (!frame.contains(blockTargetArgumentIdentifier)) {
                    auto resultReg = registerProvider.allocRegister();
                    frame.setValue(blockTargetArgumentIdentifier, resultReg);
                    return resultReg;
                } else {
                    return frame.getValue(blockTargetArgumentIdentifier);
                }
            }
        }
    }
    return registerProvider.allocRegister();
}

}// namespace NES::Nautilus::Backends::BC