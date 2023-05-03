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

#include <Nautilus/Backends/CPP/CPPLoweringProvider.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Types/AddressStamp.hpp>
#include <Nautilus/IR/Types/FloatStamp.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>
#include <utility>
#include <vector>

namespace NES::Nautilus::Backends::CPP {

CPPLoweringProvider::CPPLoweringProvider() = default;
CPPLoweringProvider::LoweringContext::LoweringContext(std::shared_ptr<IR::IRGraph> ir) : ir(std::move(ir)) {}

std::string CPPLoweringProvider::lower(std::shared_ptr<IR::IRGraph> ir) {
    auto ctx = LoweringContext(std::move(ir));
    return ctx.process().str();
}

std::string getType(const IR::Types::StampPtr& stamp) {
    if (stamp->isInteger()) {
        auto value = cast<IR::Types::IntegerStamp>(stamp);
        if (value->isSigned()) {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::BitWidth::I8: return "int8_t";
                case IR::Types::IntegerStamp::BitWidth::I16: return "int16_t";
                case IR::Types::IntegerStamp::BitWidth::I32: return "int32_t";
                case IR::Types::IntegerStamp::BitWidth::I64: return "int64_t";
            }
        } else {
            switch (value->getBitWidth()) {
                case IR::Types::IntegerStamp::BitWidth::I8: return "uint8_t";
                case IR::Types::IntegerStamp::BitWidth::I16: return "uint16_t";
                case IR::Types::IntegerStamp::BitWidth::I32: return "uint32_t";
                case IR::Types::IntegerStamp::BitWidth::I64: return "uint64_t";
            }
        }
    } else if (stamp->isFloat()) {
        auto value = cast<IR::Types::FloatStamp>(stamp);
        switch (value->getBitWidth()) {
            case IR::Types::FloatStamp::BitWidth::F32: return "float";
            case IR::Types::FloatStamp::BitWidth::F64: return "double";
        }
    } else if (stamp->isAddress()) {
        return "uint8_t*";
    } else if (stamp->isVoid()) {
        return "void";
    } else if (stamp->isBoolean()) {
        return "bool";
    }
    NES_NOT_IMPLEMENTED();
}

std::stringstream CPPLoweringProvider::LoweringContext::process() {

    auto functionOperation = ir->getRootOperation();
    RegisterFrame rootFrame;
    std::vector<std::string> arguments;
    auto functionBasicBlock = functionOperation->getFunctionBasicBlock();
    for (auto i = 0ull; i < functionBasicBlock->getArguments().size(); i++) {
        auto argument = functionBasicBlock->getArguments()[i];
        auto var = getVariable(argument->getIdentifier());
        rootFrame.setValue(argument->getIdentifier(), var);
        arguments.emplace_back(getType(argument->getStamp()) + " " + var);
    }
    this->process(functionBasicBlock, rootFrame);

    std::stringstream pipelineCode;
    pipelineCode << "\n";
    pipelineCode << "#include <cstdint>";
    pipelineCode << "\n";
    pipelineCode << "extern \"C\" auto execute(";
    for (size_t i = 0; i < arguments.size(); i++) {
        if (i != 0) {
            pipelineCode << ",";
        }
        pipelineCode << arguments[i] << " ";
    }
    pipelineCode << "){\n";
    pipelineCode << "//variable declarations\n";
    pipelineCode << blockArguments.str();
    pipelineCode << "//function definitions\n";
    pipelineCode << functions.str();
    pipelineCode << "//basic blocks\n";
    for (auto& block : blocks) {
        pipelineCode << block.str();
        pipelineCode << "\n";
    }
    pipelineCode << "}\n";

    return pipelineCode;
}

std::string CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::BasicBlock>& block, RegisterFrame& frame) {
    // assume that all argument registers are correctly set
    auto entry = activeBlocks.find(block->getIdentifier());
    if (entry == activeBlocks.end()) {

        for (auto arg : block->getArguments()) {
            if (!frame.contains(arg->getIdentifier())) {
                auto var = getVariable(arg->getIdentifier());
                blockArguments << getType(arg->getStamp()) << " " << var << ";\n";
                frame.setValue(arg->getIdentifier(), var);
            }
        }
        // create bytecode block;
        auto blockName = "Block_" + block->getIdentifier();
        short blockIndex = blocks.size();
        auto& currentBlock = blocks.emplace_back();
        currentBlock << blockName << ":\n";
        activeBlocks.emplace(block->getIdentifier(), blockName);
        for (auto& opt : block->getOperations()) {
            this->process(opt, blockIndex, frame);
        }
        return blockName;
    } else {
        return entry->second;
    }
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::AndOperation>& addOpt,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {
    auto leftInput = frame.getValue(addOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());
    auto resultVar = getVariable(addOpt->getIdentifier());
    blockArguments << getType(addOpt->getStamp()) << " " << resultVar << ";\n";
    frame.setValue(addOpt->getIdentifier(), resultVar);
    blocks[blockIndex] << resultVar << " = " << leftInput << " && " << rightInput << ";\n";
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::OrOperation>& addOpt,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {
    auto leftInput = frame.getValue(addOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());
    auto resultVar = getVariable(addOpt->getIdentifier());
    blockArguments << getType(addOpt->getStamp()) << " " << resultVar << ";\n";
    frame.setValue(addOpt->getIdentifier(), resultVar);
    blocks[blockIndex] << resultVar << " = " << leftInput << " || " << rightInput << ";\n";
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::AddOperation>& addOpt,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {
    auto leftInput = frame.getValue(addOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());
    auto resultVar = getVariable(addOpt->getIdentifier());
    blockArguments << getType(addOpt->getStamp()) << " " << resultVar << ";\n";
    frame.setValue(addOpt->getIdentifier(), resultVar);
    blocks[blockIndex] << resultVar << " = " << leftInput << " + " << rightInput << ";\n";
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::SubOperation>& subOpt,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {
    auto leftInput = frame.getValue(subOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(subOpt->getRightInput()->getIdentifier());
    auto resultVar = getVariable(subOpt->getIdentifier());
    blockArguments << getType(subOpt->getStamp()) << " " << resultVar << ";\n";
    frame.setValue(subOpt->getIdentifier(), resultVar);
    blocks[blockIndex] << resultVar << " = " << leftInput << " - " << rightInput << ";\n";
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::MulOperation>& mulOpt,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {
    auto leftInput = frame.getValue(mulOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(mulOpt->getRightInput()->getIdentifier());
    auto resultVar = getVariable(mulOpt->getIdentifier());
    blockArguments << getType(mulOpt->getStamp()) << " " << resultVar << ";\n";
    frame.setValue(mulOpt->getIdentifier(), resultVar);
    blocks[blockIndex] << resultVar << " = " << leftInput << " * " << rightInput << ";\n";
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::DivOperation>& divOp,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {
    auto leftInput = frame.getValue(divOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(divOp->getRightInput()->getIdentifier());
    auto resultVar = getVariable(divOp->getIdentifier());
    blockArguments << getType(divOp->getStamp()) << " " << resultVar << ";\n";
    frame.setValue(divOp->getIdentifier(), resultVar);
    blocks[blockIndex] << resultVar << " = " << leftInput << " / " << rightInput << ";\n";
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::CompareOperation>& cmpOp,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {
    auto leftInput = frame.getValue(cmpOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(cmpOp->getRightInput()->getIdentifier());
    auto resultVar = getVariable(cmpOp->getIdentifier());
    blockArguments << getType(cmpOp->getStamp()) << " " << resultVar << ";\n";
    frame.setValue(cmpOp->getIdentifier(), resultVar);

    if (cmpOp->isEquals() && cmpOp->getLeftInput()->getStamp()->isAddress() && cmpOp->getRightInput()->getStamp()->isInteger()) {
        blocks[blockIndex] << resultVar << " = " << leftInput << " == nullptr;\n";
        return ;
    }

    std::string comperator;
    if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::EQ) {
        comperator = " == ";
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::LT) {
        comperator = " < ";
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::GT) {
        comperator = " > ";
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::GE) {
        comperator = " >= ";
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::LE) {
        comperator = " <= ";
    } else {
        NES_NOT_IMPLEMENTED();
    }

    blocks[blockIndex] << resultVar << " = " << leftInput << comperator << rightInput << ";\n";
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::LoadOperation>& loadOp,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {
    auto address = frame.getValue(loadOp->getAddress()->getIdentifier());
    auto resultVar = getVariable(loadOp->getIdentifier());
    blockArguments << getType(loadOp->getStamp()) << " " << resultVar << ";\n";
    frame.setValue(loadOp->getIdentifier(), resultVar);
    auto type = getType(loadOp->getStamp());
    blocks[blockIndex] << resultVar << " = *reinterpret_cast<" << type << "*>(" << address << ");\n";
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::StoreOperation>& storeOp,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {
    auto address = frame.getValue(storeOp->getAddress()->getIdentifier());
    auto value = frame.getValue(storeOp->getValue()->getIdentifier());
    auto type = getType(storeOp->getValue()->getStamp());
    blocks[blockIndex] << "*reinterpret_cast<" << type << "*>(" << address << ") = " << value << ";\n";
}

void CPPLoweringProvider::LoweringContext::process(IR::Operations::BasicBlockInvocation& bi,
                                                   short blockIndex,
                                                   RegisterFrame& parentFrame) {
    auto blockInputArguments = bi.getArguments();
    auto blockTargetArguments = bi.getBlock()->getArguments();
    blocks[blockIndex] << "// prepare block arguments\n";
    for (uint64_t i = 0; i < blockInputArguments.size(); i++) {
        auto blockArgument = blockInputArguments[i]->getIdentifier();
        auto blockTargetArgument = blockTargetArguments[i]->getIdentifier();

        if (!parentFrame.contains(blockTargetArgument)) {
            auto var = getVariable(blockTargetArgument);
            parentFrame.setValue(blockTargetArgument, var);
            blockArguments << getType(blockTargetArguments[i]->getStamp()) << " " << var << ";\n";
        }

        blocks[blockIndex] << parentFrame.getValue(blockTargetArgument) << " = " << parentFrame.getValue(blockArgument) << ";\n";
    }
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::IfOperation>& ifOpt,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {
    auto conditionalReg = frame.getValue(ifOpt->getValue()->getIdentifier());
    auto trueBlock = process(ifOpt->getTrueBlockInvocation().getBlock(), frame);
    auto falseBlock = process(ifOpt->getFalseBlockInvocation().getBlock(), frame);
    blocks[blockIndex] << "if (" << conditionalReg << "){\n";
    process(ifOpt->getTrueBlockInvocation(), blockIndex, frame);
    blocks[blockIndex] << "goto " << trueBlock << ";\n";
    blocks[blockIndex] << "}else{\n";
    process(ifOpt->getFalseBlockInvocation(), blockIndex, frame);
    blocks[blockIndex] << "goto " << falseBlock << ";}\n";
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::BranchOperation>& branchOp,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {
    process(branchOp->getNextBlockInvocation(), blockIndex, frame);
    auto nextBlock = process(branchOp->getNextBlockInvocation().getBlock(), frame);
    blocks[blockIndex] << "goto " << nextBlock << ";\n";
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::Operation>& opt,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {
    switch (opt->getOperationType()) {
        case IR::Operations::Operation::OperationType::ConstBooleanOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(opt);
            auto var = getVariable(constInt->getIdentifier());
            blockArguments << getType(constInt->getStamp()) << " " << var << ";\n";
            frame.setValue(constInt->getIdentifier(), var);
            blocks[blockIndex] << var << " = " << constInt->getValue() << ";\n";
            return;
        }
        case IR::Operations::Operation::OperationType::ConstIntOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstIntOperation>(opt);
            auto var = getVariable(constInt->getIdentifier());
            frame.setValue(constInt->getIdentifier(), var);
            blockArguments << getType(constInt->getStamp()) << " " << var << ";\n";
            blocks[blockIndex] << var << " = " << constInt->getConstantIntValue() << ";\n";
            return;
        }
        case IR::Operations::Operation::OperationType::ConstFloatOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstFloatOperation>(opt);
            auto var = getVariable(constInt->getIdentifier());
            frame.setValue(constInt->getIdentifier(), var);
            blockArguments << getType(constInt->getStamp()) << " " << var << ";\n";
            blocks[blockIndex] << var << " = " << constInt->getConstantFloatValue() << ";\n";
            return;
        }
        case IR::Operations::Operation::OperationType::AddOp: {
            auto addOpt = std::static_pointer_cast<IR::Operations::AddOperation>(opt);
            process(addOpt, blockIndex, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::MulOp: {
            auto mulOpt = std::static_pointer_cast<IR::Operations::MulOperation>(opt);
            process(mulOpt, blockIndex, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::SubOp: {
            auto subOpt = std::static_pointer_cast<IR::Operations::SubOperation>(opt);
            process(subOpt, blockIndex, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::DivOp: {
            auto divOpt = std::static_pointer_cast<IR::Operations::DivOperation>(opt);
            process(divOpt, blockIndex, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::ReturnOp: {
            auto returnOpt = std::static_pointer_cast<IR::Operations::ReturnOperation>(opt);
            if (returnOpt->hasReturnValue()) {
                auto returnFOp = frame.getValue(returnOpt->getReturnValue()->getIdentifier());
                blocks[blockIndex] << "return " << returnFOp << ";\n";
            } else {
                blocks[blockIndex] << "return;\n";
            }
            return;
        }
        case IR::Operations::Operation::OperationType::CompareOp: {
            auto compOpt = std::static_pointer_cast<IR::Operations::CompareOperation>(opt);
            process(compOpt, blockIndex, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::IfOp: {
            auto ifOpt = std::static_pointer_cast<IR::Operations::IfOperation>(opt);
            process(ifOpt, blockIndex, frame);
            return;
        }

        case IR::Operations::Operation::OperationType::BranchOp: {
            auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(opt);
            process(branchOp, blockIndex, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::LoadOp: {
            auto load = std::static_pointer_cast<IR::Operations::LoadOperation>(opt);
            process(load, blockIndex, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::StoreOp: {
            auto store = std::static_pointer_cast<IR::Operations::StoreOperation>(opt);
            process(store, blockIndex, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::ProxyCallOp: {
            auto call = std::static_pointer_cast<IR::Operations::ProxyCallOperation>(opt);
            process(call, blockIndex, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::OrOp: {
            auto call = std::static_pointer_cast<IR::Operations::OrOperation>(opt);
            process(call, blockIndex, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::AndOp: {
            auto call = std::static_pointer_cast<IR::Operations::AndOperation>(opt);
            process(call, blockIndex, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::NegateOp: {
            auto call = std::static_pointer_cast<IR::Operations::NegateOperation>(opt);
            process(call, blockIndex, frame);
            return;
        }
        case IR::Operations::Operation::OperationType::CastOp: {
            auto cast = std::static_pointer_cast<IR::Operations::CastOperation>(opt);
            process(cast, blockIndex, frame);
            return;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("Operation " << opt->toString() << " not handled");
            return;
        }
    }
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::ProxyCallOperation>& opt,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {

    auto returnType = getType(opt->getStamp());
    std::stringstream argTypes;
    std::stringstream args;
    for (size_t i = 0; i < opt->getInputArguments().size(); i++) {
        auto arg = opt->getInputArguments()[i];

        if (i != 0) {
            argTypes << ",";
            args << ",";
        }
        args << frame.getValue(arg->getIdentifier());
        argTypes << getType(arg->getStamp());
    }
    if (!functionNames.contains(opt->getFunctionSymbol())) {
        functions << "auto " << opt->getFunctionSymbol() << " = "
                  << "(" << returnType << "(*)(" << argTypes.str() << "))" << opt->getFunctionPtr() << ";\n";
        functionNames.emplace(opt->getFunctionSymbol());
    }
    if (!opt->getStamp()->isVoid()) {
        auto resultVar = getVariable(opt->getIdentifier());
        blockArguments << getType(opt->getStamp()) << " " << resultVar << ";\n";
        frame.setValue(opt->getIdentifier(), resultVar);
        blocks[blockIndex] << resultVar << " = ";
    }
    blocks[blockIndex] << opt->getFunctionSymbol() << "(" << args.str() << ");\n";
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::NegateOperation>& negateOperation,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {
    auto input = frame.getValue(negateOperation->getInput()->getIdentifier());
    auto resultVar = getVariable(negateOperation->getIdentifier());
    blockArguments << getType(negateOperation->getStamp()) << " " << resultVar << ";\n";
    frame.setValue(negateOperation->getIdentifier(), resultVar);
    blocks[blockIndex] << resultVar << "= !" << input << ";\n";
}

void CPPLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::CastOperation>& castOp,
                                                   short blockIndex,
                                                   RegisterFrame& frame) {
    auto input = frame.getValue(castOp->getInput()->getIdentifier());
    auto var = getVariable(castOp->getIdentifier());
    frame.setValue(castOp->getIdentifier(), var);
    auto targetType = getType(castOp->getStamp());
    blockArguments << targetType << " " << var << ";\n";
    blocks[blockIndex] << var << " = (" << targetType << ")" << input << ";\n";
}

std::string CPPLoweringProvider::LoweringContext::getVariable(const std::string& id) { return "var_" + id; }

}// namespace NES::Nautilus::Backends::CPP