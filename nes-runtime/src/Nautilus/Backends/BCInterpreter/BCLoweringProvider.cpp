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

#include "Nautilus/IR/Types/AddressStamp.hpp"
#include "Nautilus/IR/Types/FloatStamp.hpp"
#include "Nautilus/IR/Types/IntegerStamp.hpp"
#include <Nautilus/Backends/BCInterpreter/BCLoweringProvider.hpp>
#include <utility>

namespace NES::Nautilus::Backends::BC {

BCLoweringProvider::BCLoweringProvider() {}
BCLoweringProvider::LoweringContext::LoweringContext(std::shared_ptr<IR::IRGraph> ir) : ir(std::move(ir)) {}

std::tuple<Code, RegisterFile> BCLoweringProvider::lower(std::shared_ptr<IR::IRGraph> ir) {
    auto ctx = LoweringContext(ir);
    return ctx.process();
}

short BCLoweringProvider::RegisterProvider::allocRegister() { return currentRegister++; }

void BCLoweringProvider::RegisterProvider::freeRegister() {
    // TODO
}

std::tuple<Code, RegisterFile> BCLoweringProvider::LoweringContext::process() {
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

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::AddOperation>& addOpt,
                                                  short block,
                                                  RegisterFrame& frame) {
    auto leftInput = frame.getValue(addOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());
    auto resultReg = registerProvider.allocRegister();
    frame.setValue(addOpt->getIdentifier(), resultReg);
    ByteCode bc = ByteCode::ADD_f;
    auto type = addOpt->getStamp();
    if (type->isInteger()) {
        auto value = cast<IR::Types::IntegerStamp>(type);
        if (value->isSigned()) {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: bc = ByteCode::ADD_i8; break;
                case IR::Types::IntegerStamp::I16: bc = ByteCode::ADD_i16; break;
                case IR::Types::IntegerStamp::I32: bc = ByteCode::ADD_i32; break;
                case IR::Types::IntegerStamp::I64: bc = ByteCode::ADD_i64; break;
            }
        } else {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: bc = ByteCode::ADD_ui8; break;
                case IR::Types::IntegerStamp::I16: bc = ByteCode::ADD_ui16; break;
                case IR::Types::IntegerStamp::I32: bc = ByteCode::ADD_ui32; break;
                case IR::Types::IntegerStamp::I64: bc = ByteCode::ADD_ui64; break;
            }
        }
    } else if (type->isFloat()) {
        auto value = cast<IR::Types::FloatStamp>(type);
        switch (value->getBitWidth()) {
            case IR::Types::FloatStamp::F32: bc = ByteCode::ADD_f; break;
            case IR::Types::FloatStamp::F64: bc = ByteCode::ADD_d; break;
        }
    } else if (type->isAddress()) {
        auto value = cast<IR::Types::AddressStamp>(type);
        bc = ByteCode::ADD_i64;
    }
    OpCode oc = {bc, leftInput, rightInput, resultReg};
    program.blocks[block].code.emplace_back(oc);
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::SubOperation>& subOpt,
                                                  short block,
                                                  RegisterFrame& frame) {
    auto leftInput = frame.getValue(subOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(subOpt->getRightInput()->getIdentifier());
    auto resultReg = registerProvider.allocRegister();
    frame.setValue(subOpt->getIdentifier(), resultReg);
    ByteCode bc = ByteCode::SUB_d;
    auto type = subOpt->getStamp();
    if (type->isInteger()) {
        auto value = cast<IR::Types::IntegerStamp>(type);
        if (value->isSigned()) {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: bc = ByteCode::SUB_i8; break;
                case IR::Types::IntegerStamp::I16: bc = ByteCode::SUB_i16; break;
                case IR::Types::IntegerStamp::I32: bc = ByteCode::SUB_i32; break;
                case IR::Types::IntegerStamp::I64: bc = ByteCode::SUB_i64; break;
            }
        } else {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: bc = ByteCode::SUB_ui8; break;
                case IR::Types::IntegerStamp::I16: bc = ByteCode::SUB_ui16; break;
                case IR::Types::IntegerStamp::I32: bc = ByteCode::SUB_ui32; break;
                case IR::Types::IntegerStamp::I64: bc = ByteCode::SUB_ui64; break;
            }
        }
    } else if (type->isFloat()) {
        auto value = cast<IR::Types::FloatStamp>(type);
        switch (value->getBitWidth()) {
            case IR::Types::FloatStamp::F32: bc = ByteCode::SUB_f; break;
            case IR::Types::FloatStamp::F64: bc = ByteCode::SUB_d; break;
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
    auto resultReg = registerProvider.allocRegister();
    frame.setValue(mulOpt->getIdentifier(), resultReg);
    ByteCode bc = ByteCode::MUL_d;
    auto type = mulOpt->getStamp();
    if (type->isInteger()) {
        auto value = cast<IR::Types::IntegerStamp>(type);
        if (value->isSigned()) {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: bc = ByteCode::MUL_i8; break;
                case IR::Types::IntegerStamp::I16: bc = ByteCode::MUL_i16; break;
                case IR::Types::IntegerStamp::I32: bc = ByteCode::MUL_i32; break;
                case IR::Types::IntegerStamp::I64: bc = ByteCode::MUL_i64; break;
            }
        } else {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: bc = ByteCode::MUL_ui8; break;
                case IR::Types::IntegerStamp::I16: bc = ByteCode::MUL_ui16; break;
                case IR::Types::IntegerStamp::I32: bc = ByteCode::MUL_ui32; break;
                case IR::Types::IntegerStamp::I64: bc = ByteCode::MUL_ui64; break;
            }
        }
    } else if (type->isFloat()) {
        auto value = cast<IR::Types::FloatStamp>(type);
        switch (value->getBitWidth()) {
            case IR::Types::FloatStamp::F32: bc = ByteCode::MUL_f; break;
            case IR::Types::FloatStamp::F64: bc = ByteCode::MUL_d; break;
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
    auto resultReg = registerProvider.allocRegister();
    frame.setValue(divOp->getIdentifier(), resultReg);
    ByteCode bc = ByteCode::DIV_d;
    auto type = divOp->getStamp();
    if (type->isInteger()) {
        auto value = cast<IR::Types::IntegerStamp>(type);
        if (value->isSigned()) {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: bc = ByteCode::DIV_i8; break;
                case IR::Types::IntegerStamp::I16: bc = ByteCode::DIV_i16; break;
                case IR::Types::IntegerStamp::I32: bc = ByteCode::DIV_i32; break;
                case IR::Types::IntegerStamp::I64: bc = ByteCode::DIV_i64; break;
            }
        } else {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: bc = ByteCode::DIV_ui8; break;
                case IR::Types::IntegerStamp::I16: bc = ByteCode::DIV_ui16; break;
                case IR::Types::IntegerStamp::I32: bc = ByteCode::DIV_ui32; break;
                case IR::Types::IntegerStamp::I64: bc = ByteCode::DIV_ui64; break;
            }
        }
    } else if (type->isFloat()) {
        auto value = cast<IR::Types::FloatStamp>(type);
        switch (value->getBitWidth()) {
            case IR::Types::FloatStamp::F32: bc = ByteCode::DIV_f; break;
            case IR::Types::FloatStamp::F64: bc = ByteCode::DIV_d; break;
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
    auto resultReg = registerProvider.allocRegister();
    frame.setValue(cmpOp->getIdentifier(), resultReg);

    if (cmpOp->getComparator() == IR::Operations::CompareOperation::IEQ) {
        ByteCode bc = ByteCode::EQ_d;
        auto type = cmpOp->getLeftInput()->getStamp();
        if (type->isInteger()) {
            auto value = cast<IR::Types::IntegerStamp>(type);
            if (value->isSigned()) {
                switch (value->getBitWidth()) {
                    case IR::Types::IntegerStamp::I8: bc = ByteCode::EQ_i8; break;
                    case IR::Types::IntegerStamp::I16: bc = ByteCode::EQ_i16; break;
                    case IR::Types::IntegerStamp::I32: bc = ByteCode::EQ_i32; break;
                    case IR::Types::IntegerStamp::I64: bc = ByteCode::EQ_i64; break;
                }
            } else {
                switch (value->getBitWidth()) {
                    case IR::Types::IntegerStamp::I8: bc = ByteCode::EQ_ui8; break;
                    case IR::Types::IntegerStamp::I16: bc = ByteCode::EQ_ui16; break;
                    case IR::Types::IntegerStamp::I32: bc = ByteCode::EQ_ui32; break;
                    case IR::Types::IntegerStamp::I64: bc = ByteCode::EQ_ui64; break;
                }
            }
        } else if (type->isFloat()) {
            auto value = cast<IR::Types::FloatStamp>(type);
            switch (value->getBitWidth()) {
                case IR::Types::FloatStamp::F32: bc = ByteCode::EQ_f; break;
                case IR::Types::FloatStamp::F64: bc = ByteCode::EQ_d; break;
            }
        }
        OpCode oc = {bc, leftInput, rightInput, resultReg};
        program.blocks[block].code.emplace_back(oc);
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::ISLT) {
        ByteCode bc = ByteCode::LESS_THAN_d;
        auto type = cmpOp->getLeftInput()->getStamp();
        if (type->isInteger()) {
            auto value = cast<IR::Types::IntegerStamp>(type);
            if (value->isSigned()) {
                switch (value->getBitWidth()) {
                    case IR::Types::IntegerStamp::I8: bc = ByteCode::LESS_THAN_i8; break;
                    case IR::Types::IntegerStamp::I16: bc = ByteCode::LESS_THAN_i16; break;
                    case IR::Types::IntegerStamp::I32: bc = ByteCode::LESS_THAN_i32; break;
                    case IR::Types::IntegerStamp::I64: bc = ByteCode::LESS_THAN_i64; break;
                }
            } else {
                switch (value->getBitWidth()) {
                    case IR::Types::IntegerStamp::I8: bc = ByteCode::LESS_THAN_ui8; break;
                    case IR::Types::IntegerStamp::I16: bc = ByteCode::LESS_THAN_ui16; break;
                    case IR::Types::IntegerStamp::I32: bc = ByteCode::LESS_THAN_ui32; break;
                    case IR::Types::IntegerStamp::I64: bc = ByteCode::LESS_THAN_ui64; break;
                }
            }
        } else if (type->isFloat()) {
            auto value = cast<IR::Types::FloatStamp>(type);
            switch (value->getBitWidth()) {
                case IR::Types::FloatStamp::F32: bc = ByteCode::LESS_THAN_f; break;
                case IR::Types::FloatStamp::F64: bc = ByteCode::LESS_THAN_d; break;
            }
        }
        OpCode oc = {bc, leftInput, rightInput, resultReg};
        program.blocks[block].code.emplace_back(oc);
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::ISGT) {
        ByteCode bc = ByteCode::GREATER_THAN_d;
        auto type = cmpOp->getLeftInput()->getStamp();
        if (type->isInteger()) {
            auto value = cast<IR::Types::IntegerStamp>(type);
            if (value->isSigned()) {
                switch (value->getBitWidth()) {
                    case IR::Types::IntegerStamp::I8: bc = ByteCode::GREATER_THAN_i8; break;
                    case IR::Types::IntegerStamp::I16: bc = ByteCode::GREATER_THAN_i16; break;
                    case IR::Types::IntegerStamp::I32: bc = ByteCode::GREATER_THAN_i32; break;
                    case IR::Types::IntegerStamp::I64: bc = ByteCode::GREATER_THAN_i64; break;
                }
            } else {
                switch (value->getBitWidth()) {
                    case IR::Types::IntegerStamp::I8: bc = ByteCode::GREATER_THAN_ui8; break;
                    case IR::Types::IntegerStamp::I16: bc = ByteCode::GREATER_THAN_ui16; break;
                    case IR::Types::IntegerStamp::I32: bc = ByteCode::GREATER_THAN_ui32; break;
                    case IR::Types::IntegerStamp::I64: bc = ByteCode::GREATER_THAN_ui64; break;
                }
            }
        } else if (type->isFloat()) {
            auto value = cast<IR::Types::FloatStamp>(type);
            switch (value->getBitWidth()) {
                case IR::Types::FloatStamp::F32: bc = ByteCode::GREATER_THAN_f; break;
                case IR::Types::FloatStamp::F64: bc = ByteCode::GREATER_THAN_d; break;
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
    auto resultReg = registerProvider.allocRegister();
    frame.setValue(loadOp->getIdentifier(), resultReg);
    ByteCode bc = ByteCode::LOAD_d;
    auto type = loadOp->getStamp();
    if (type->isInteger()) {
        auto value = cast<IR::Types::IntegerStamp>(type);
        if (value->isSigned()) {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: bc = ByteCode::LOAD_i8; break;
                case IR::Types::IntegerStamp::I16: bc = ByteCode::LOAD_i16; break;
                case IR::Types::IntegerStamp::I32: bc = ByteCode::LOAD_i32; break;
                case IR::Types::IntegerStamp::I64: bc = ByteCode::LOAD_i64; break;
            }
        } else {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: bc = ByteCode::LOAD_ui8; break;
                case IR::Types::IntegerStamp::I16: bc = ByteCode::LOAD_ui16; break;
                case IR::Types::IntegerStamp::I32: bc = ByteCode::LOAD_ui32; break;
                case IR::Types::IntegerStamp::I64: bc = ByteCode::LOAD_ui64; break;
            }
        }
    } else if (type->isFloat()) {
        auto value = cast<IR::Types::FloatStamp>(type);
        switch (value->getBitWidth()) {
            case IR::Types::FloatStamp::F32: bc = ByteCode::LOAD_f; break;
            case IR::Types::FloatStamp::F64: bc = ByteCode::LOAD_d; break;
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
    ByteCode bc = ByteCode::STORE_d;
    auto type = storeOp->getValue()->getStamp();
    if (type->isInteger()) {
        auto value = cast<IR::Types::IntegerStamp>(type);
        if (value->isSigned()) {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: bc = ByteCode::STORE_i8; break;
                case IR::Types::IntegerStamp::I16: bc = ByteCode::STORE_i16; break;
                case IR::Types::IntegerStamp::I32: bc = ByteCode::STORE_i32; break;
                case IR::Types::IntegerStamp::I64: bc = ByteCode::STORE_i64; break;
            }
        } else {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: bc = ByteCode::STORE_ui8; break;
                case IR::Types::IntegerStamp::I16: bc = ByteCode::STORE_ui16; break;
                case IR::Types::IntegerStamp::I32: bc = ByteCode::STORE_ui32; break;
                case IR::Types::IntegerStamp::I64: bc = ByteCode::STORE_ui64; break;
            }
        }
    } else if (type->isFloat()) {
        auto value = cast<IR::Types::FloatStamp>(type);
        switch (value->getBitWidth()) {
            case IR::Types::FloatStamp::F32: bc = ByteCode::STORE_f; break;
            case IR::Types::FloatStamp::F64: bc = ByteCode::STORE_d; break;
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
            auto resultReg = registerProvider.allocRegister();
            // TODO use child frame
            parentFrame.setValue(blockTargetArgument, resultReg);
            OpCode oc = {ByteCode::REG_MOV, parentFrameReg, -1, resultReg};
            program.blocks[block].code.emplace_back(oc);
        } else {
            // TODO use child frame
            auto resultReg = parentFrame.getValue(blockTargetArgument);
            OpCode oc = {ByteCode::REG_MOV, parentFrameReg, -1, resultReg};
            program.blocks[block].code.emplace_back(oc);
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
    program.blocks[block].terminatorOp = JumpOp{blockIndex};
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::Operation>& opt,
                                                  short block,
                                                  RegisterFrame& frame) {
    switch (opt->getOperationType()) {
        case IR::Operations::Operation::ConstBooleanOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(opt);
            auto defaultRegister = registerProvider.allocRegister();
            frame.setValue(constInt->getIdentifier(), defaultRegister);
            defaultRegisterFile[defaultRegister] = constInt->getValue();
            return;
        }
        case IR::Operations::Operation::ConstIntOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstIntOperation>(opt);
            auto defaultRegister = registerProvider.allocRegister();
            frame.setValue(constInt->getIdentifier(), defaultRegister);
            defaultRegisterFile[defaultRegister] = constInt->getConstantIntValue();
            return;
        }
        case IR::Operations::Operation::ConstFloatOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstFloatOperation>(opt);
            auto defaultRegister = registerProvider.allocRegister();
            frame.setValue(constInt->getIdentifier(), defaultRegister);
            if (cast<IR::Types::FloatStamp>(constInt->getStamp())->getBitWidth() == IR::Types::FloatStamp::F32) {
                auto floatValue = (float) constInt->getConstantFloatValue();
                auto floatReg = reinterpret_cast<float*>(&defaultRegisterFile[defaultRegister]);
                *floatReg = floatValue;
            } else {
                auto floatValue = (double) constInt->getConstantFloatValue();
                auto floatReg = reinterpret_cast<double*>(&defaultRegisterFile[defaultRegister]);
                *floatReg = floatValue;
            }
            return;
        }
        case IR::Operations::Operation::AddOp: {
            auto addOpt = std::static_pointer_cast<IR::Operations::AddOperation>(opt);
            process(addOpt, block, frame);
            return;
        }
        case IR::Operations::Operation::MulOp: {
            auto mulOpt = std::static_pointer_cast<IR::Operations::MulOperation>(opt);
            process(mulOpt, block, frame);
            return;
        }
        case IR::Operations::Operation::SubOp: {
            auto subOpt = std::static_pointer_cast<IR::Operations::SubOperation>(opt);
            process(subOpt, block, frame);
            return;
        }
        case IR::Operations::Operation::DivOp: {
            auto divOpt = std::static_pointer_cast<IR::Operations::DivOperation>(opt);
            process(divOpt, block, frame);
            return;
        }
        case IR::Operations::Operation::ReturnOp: {
            auto returnOpt = std::static_pointer_cast<IR::Operations::ReturnOperation>(opt);
            if (returnOpt->hasReturnValue()) {
                auto returnFOp = frame.getValue(returnOpt->getReturnValue()->getIdentifier());
                program.blocks[block].terminatorOp = ReturnOp{returnFOp};
                auto type = returnOpt->getReturnValue()->getStamp();
                if (type->isInteger()) {
                    auto value = cast<IR::Types::IntegerStamp>(type);
                    switch (value->getBitWidth()) {
                        case IR::Types::IntegerStamp::I8: program.returnType = Type::i8; break;
                        case IR::Types::IntegerStamp::I16: program.returnType = Type::i16; break;
                        case IR::Types::IntegerStamp::I32: program.returnType = Type::i32; break;
                        case IR::Types::IntegerStamp::I64: program.returnType = Type::i64; break;
                    }
                } else if (type->isFloat()) {
                    auto value = cast<IR::Types::FloatStamp>(type);
                    switch (value->getBitWidth()) {
                        case IR::Types::FloatStamp::F32: program.returnType = Type::f; break;
                        case IR::Types::FloatStamp::F64: program.returnType = Type::d; break;
                    }
                } else if (type->isBoolean()) {
                    program.returnType = Type::b;
                }
                return;
            } else {
                program.blocks[block].terminatorOp = ReturnOp{-1};
                return;
            }
        }
        case IR::Operations::Operation::CompareOp: {
            auto compOpt = std::static_pointer_cast<IR::Operations::CompareOperation>(opt);
            process(compOpt, block, frame);
            return;
        }
        case IR::Operations::Operation::IfOp: {
            auto ifOpt = std::static_pointer_cast<IR::Operations::IfOperation>(opt);
            process(ifOpt, block, frame);
            return;
        }

        case IR::Operations::Operation::BranchOp: {
            auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(opt);
            process(branchOp, block, frame);
            return;
        }
        case IR::Operations::Operation::LoadOp: {
            auto load = std::static_pointer_cast<IR::Operations::LoadOperation>(opt);
            process(load, block, frame);
            return;
        }
        case IR::Operations::Operation::StoreOp: {
            auto store = std::static_pointer_cast<IR::Operations::StoreOperation>(opt);
            process(store, block, frame);
            return;
        }
        case IR::Operations::Operation::ProxyCallOp: {
            auto call = std::static_pointer_cast<IR::Operations::ProxyCallOperation>(opt);
            process(call, block, frame);
            return;
        }
            /*
        case IR::Operations::Operation::LoopOp: {
            auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(opt);
            process(loopOp, frame);
            return;
        }


        case IR::Operations::Operation::OrOp: {
            auto call = std::static_pointer_cast<IR::Operations::OrOperation>(opt);
            process(call, frame);
            return;
        }
        case IR::Operations::Operation::AndOp: {
            auto call = std::static_pointer_cast<IR::Operations::AndOperation>(opt);
            process(call, frame);
            return;
        }
             */
        default: {
            NES_THROW_RUNTIME_ERROR("Operation " << opt->toString() << " not handled");
            return;
        }
    }
}

Type getType(IR::Types::StampPtr stamp) {
    if (stamp->isInteger()) {
        auto value = cast<IR::Types::IntegerStamp>(stamp);
        if (value->isSigned()) {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: return Type::i8;
                case IR::Types::IntegerStamp::I16: return Type::i16;
                case IR::Types::IntegerStamp::I32: return Type::i32;
                case IR::Types::IntegerStamp::I64: return Type::i64;
            }
        } else {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: return Type::i8;
                case IR::Types::IntegerStamp::I16: return Type::i16;
                case IR::Types::IntegerStamp::I32: return Type::i32;
                case IR::Types::IntegerStamp::I64: return Type::i64;
            }
        }
    } else if (stamp->isFloat()) {
        auto value = cast<IR::Types::FloatStamp>(stamp);
        switch (value->getBitWidth()) {
            case IR::Types::FloatStamp::F32: return Type::f;
            case IR::Types::FloatStamp::F64: return Type::d;
        }
    } else if (stamp->isAddress()) {
        return Type::ptr;
    } else if (stamp->isVoid()) {
        return Type::v;
    } else if (stamp->isBoolean()) {
        return Type::v;
    }
    NES_NOT_IMPLEMENTED();
}

void BCLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::ProxyCallOperation>& opt,
                                                  short block,
                                                  RegisterFrame& frame) {

    auto arguments = opt->getInputArguments();
    ByteCode bc;
    if (opt->getStamp()->isVoid()) {
        if (arguments.empty()) {
            bc = ByteCode::CALL_v;
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
            } else {
                NES_NOT_IMPLEMENTED();
            }
        } else if (arguments.size() == 2) {
            if (getType(arguments[0]->getStamp()) == Type::i64 && getType(arguments[1]->getStamp()) == Type::i64) {
                bc = ByteCode::CALL_i64_i64_i64;
            } else {
                NES_NOT_IMPLEMENTED();
            }
        } else {
            NES_NOT_IMPLEMENTED();
        }
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
        resultRegister = registerProvider.allocRegister();
        frame.setValue(opt->getIdentifier(), resultRegister);
    }
    OpCode oc = {bc, funcInfoRegister, -1, resultRegister};
    program.blocks[block].code.emplace_back(oc);
}

}// namespace NES::Nautilus::Backends::BC