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

#include <Nautilus/Backends/CPP/CPPLoweringInterface.hpp>

#include <Nautilus/CodeGen/CPP/IfElseSegment.hpp>
#include <Nautilus/IR/Types/FloatStamp.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Backends::CPP {

CPPLoweringInterface::CPPLoweringInterface(const RegisterFrame& frame)
    : functionBody(std::make_unique<CodeGen::Segment>())
    , registerFrame(frame)
{

}

std::unique_ptr<CodeGen::Segment> CPPLoweringInterface::lowerGraph(const std::shared_ptr<IR::IRGraph>& irGraph) {
    auto functionOperation = irGraph->getRootOperation();
    auto functionBasicBlock = functionOperation->getFunctionBasicBlock();
    for (auto i = 0ull; i < functionBasicBlock->getArguments().size(); i++) {
        auto argument = functionBasicBlock->getArguments()[i];
        auto var = getVariable(argument->getIdentifier());
        registerFrame.setValue(argument->getIdentifier(), var);
    }

    auto _ = lowerBasicBlock(functionBasicBlock, registerFrame);
    functionBody->mergeProloguesToTopLevel();
    return std::move(functionBody);
}

std::string CPPLoweringInterface::getType(const IR::Types::StampPtr& stamp) {
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

std::string CPPLoweringInterface::getVariable(const std::string &id) {
    return "var_" + id;
}

std::shared_ptr<CodeGen::CPP::LabeledSegment> CPPLoweringInterface::lowerBasicBlock(const std::shared_ptr<IR::BasicBlock>& block, RegisterFrame& frame) {
    // assume that all argument registers are correctly set
    auto blockName = "Block_" + block->getIdentifier();
    auto entry = activeBlocks.find(blockName);
    if (entry != activeBlocks.end()) {
        auto loweredBlock = std::dynamic_pointer_cast<CodeGen::CPP::LabeledSegment>(entry->second);
        return loweredBlock;
    }

    auto labeledSegment = std::make_shared<CodeGen::CPP::LabeledSegment>(blockName);
    for (auto& arg : block->getArguments()) {
        if (!frame.contains(arg->getIdentifier())) {
            auto type = getType(arg->getStamp());
            auto var = getVariable(arg->getIdentifier());
            auto stmt = std::make_shared<CodeGen::CPP::Statement>(type + " " + var);
            labeledSegment->addToProlog(stmt);
            frame.setValue(arg->getIdentifier(), var);
        }
    }
    if (activeBlocks.find(labeledSegment->getLabel()) == activeBlocks.end()) {
        functionBody->add(labeledSegment);
    }
    activeBlocks.emplace(blockName, labeledSegment);
    for (auto& opt : block->getOperations()) {
        auto loweredOp = lowerOperation(opt, frame);
        labeledSegment->add(std::move(loweredOp));
    }
    return labeledSegment;
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerAdd(const std::shared_ptr<IR::Operations::AddOperation>& operation, IRLoweringInterface::RegisterFrame& frame) {
    return lowerBinaryOperation<IR::Operations::AddOperation>(operation, "+", frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerAddress(const std::shared_ptr<IR::Operations::AddressOperation>&, RegisterFrame&) {
    NES_NOT_IMPLEMENTED();
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerAnd(const std::shared_ptr<IR::Operations::AndOperation>& operation, RegisterFrame& frame) {
    return lowerBinaryOperation<IR::Operations::AndOperation>(operation, "&&", frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerBasicBlockArgument(const std::shared_ptr<IR::Operations::BasicBlockArgument>&, RegisterFrame&) {
    NES_NOT_IMPLEMENTED();
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerBasicBlockInvocation(const std::shared_ptr<IR::Operations::BasicBlockInvocation>& operation,
                                                  IRLoweringInterface::RegisterFrame& frame) {
    auto blockInputArguments = operation->getArguments();
    auto blockTargetArguments = operation->getBlock()->getArguments();
    auto code = std::make_unique<CodeGen::Segment>();
    for (uint64_t i = 0; i < blockInputArguments.size(); i++) {
        auto blockArgument = blockInputArguments[i]->getIdentifier();
        auto blockTargetArgument = blockTargetArguments[i]->getIdentifier();

        if (!frame.contains(blockTargetArgument)) {
            auto var = getVariable(blockTargetArgument);
            frame.setValue(blockTargetArgument, var);
            auto type = getType(blockTargetArguments[i]->getStamp());
            auto decl = std::make_shared<CodeGen::CPP::Statement>(type + " " + var);
            code->addToProlog(decl);
        }

        auto resultVar = frame.getValue(blockTargetArgument);
        auto var = frame.getValue(blockArgument);
        auto stmt = std::make_shared<CodeGen::CPP::Statement>(resultVar + " = " + var);
        code->add(stmt);
    }
    return std::move(code);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerBitwiseAnd(const std::shared_ptr<IR::Operations::BitWiseAndOperation>& operation, RegisterFrame& frame) {
    return lowerBinaryOperation<IR::Operations::BitWiseAndOperation>(operation, "&", frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerBitwiseLeftShift(const std::shared_ptr<IR::Operations::BitWiseLeftShiftOperation>& operation, RegisterFrame& frame) {
    return lowerBinaryOperation<IR::Operations::BitWiseLeftShiftOperation>(operation, "<<", frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerBitwiseOr(const std::shared_ptr<IR::Operations::BitWiseOrOperation>& operation, RegisterFrame& frame) {
    return lowerBinaryOperation<IR::Operations::BitWiseOrOperation>(operation, "|", frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerBitwiseRightShift(const std::shared_ptr<IR::Operations::BitWiseRightShiftOperation>& operation, RegisterFrame& frame) {
    return lowerBinaryOperation<IR::Operations::BitWiseRightShiftOperation>(operation, ">>", frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerBitwiseXor(const std::shared_ptr<IR::Operations::BitWiseXorOperation>& operation, RegisterFrame& frame) {
    return lowerBinaryOperation<IR::Operations::BitWiseXorOperation>(operation, "^", frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerBranch(const std::shared_ptr<IR::Operations::BranchOperation>& operation, RegisterFrame& frame) {
    auto nextBlockInvocation = std::make_shared<IR::Operations::BasicBlockInvocation>(operation->getNextBlockInvocation());
    auto code = std::make_unique<CodeGen::Segment>();
    auto loweredInvoke = lowerBasicBlockInvocation(nextBlockInvocation, frame);
    code->add(std::move(loweredInvoke));
    auto nextBlock = nextBlockInvocation->getBlock();
    auto loweredBlock = lowerBasicBlock(nextBlock, frame);
    auto blockLabel = loweredBlock->getLabel();
    auto gotoStmt = std::make_shared<CodeGen::CPP::Statement>("goto " + blockLabel);
    if (activeBlocks.find(loweredBlock->getLabel()) == activeBlocks.end()) {
        functionBody->add(loweredBlock);
    }
    code->add(gotoStmt);
    return std::move(code);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerBuiltInVariable(const std::shared_ptr<IR::Operations::BuiltInVariableOperation>& operation, RegisterFrame& frame) {
    return lowerConstOperation<IR::Operations::BuiltInVariableOperation>(operation, frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerCast(const std::shared_ptr<IR::Operations::CastOperation>& operation, RegisterFrame& frame) {
    auto input = frame.getValue(operation->getInput()->getIdentifier());
    auto var = getVariable(operation->getIdentifier());
    frame.setValue(operation->getIdentifier(), var);
    auto targetType = getType(operation->getStamp());
    auto code = std::make_unique<CodeGen::Segment>();
    auto decl = std::make_shared<CodeGen::CPP::Statement>(targetType + " " + var);
    code->addToProlog(decl);
    auto stmt = std::make_shared<CodeGen::CPP::Statement>(var + " = (" + targetType + ")" + input);
    code->add(stmt);
    return std::move(code);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerCompare(const std::shared_ptr<IR::Operations::CompareOperation>& operation, RegisterFrame& frame) {
    auto lhs = operation->getLeftInput();
    // we have to handle the special case that we want to do a null check. Currently, Nautilus IR just contains x == 0, thus we check if x is a ptr type.
    if (operation->isEquals() && lhs->getStamp()->isAddress() && operation->getRightInput()->getStamp()->isInteger()) {
        auto leftInput = frame.getValue(lhs->getIdentifier());
        auto var = getVariable(operation->getIdentifier());
        auto targetType = getType(operation->getStamp());
        auto code = std::make_unique<CodeGen::Segment>();
        auto decl = std::make_shared<CodeGen::CPP::Statement>(targetType + " " + var);
        code->addToProlog(decl);
        auto stmt = std::make_shared<CodeGen::CPP::Statement>(var + " = " + leftInput + " == nullptr");
        code->add(stmt);
        return std::move(code);
    }

    std::string binaryOp;
    switch (operation->getComparator()) {
        case IR::Operations::CompareOperation::EQ: {
            binaryOp = "==";
            break;
        }
        case IR::Operations::CompareOperation::NE: {
            binaryOp = "!=";
            break;
        }
        case IR::Operations::CompareOperation::LT: {
            binaryOp = "<";
            break;
        }
        case IR::Operations::CompareOperation::LE: {
            binaryOp = "<=";
            break;
        }
        case IR::Operations::CompareOperation::GT: {
            binaryOp = ">";
            break;
        }
        case IR::Operations::CompareOperation::GE: {
            binaryOp = ">=";
            break;
        }
        default: {
            NES_NOT_IMPLEMENTED();
        }
    }

    return lowerBinaryOperation<IR::Operations::CompareOperation>(operation, binaryOp, frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerConstAddress(const std::shared_ptr<IR::Operations::ConstAddressOperation>& operation, RegisterFrame& frame) {
    auto var = getVariable(operation->getIdentifier());
    auto type = getType(operation->getStamp());
    frame.setValue(operation->getIdentifier(), var);
    auto code = std::make_unique<CodeGen::Segment>();
    auto decl = std::make_shared<CodeGen::CPP::Statement>(type + " " + var);
    code->addToProlog(decl);
    auto stmt = std::make_shared<CodeGen::CPP::Statement>(fmt::format("{} = reinterpret_cast<uint8_t*>({})", var, operation->getValue()));
    code->add(stmt);
    return std::move(code);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerConstBoolean(const std::shared_ptr<IR::Operations::ConstBooleanOperation>& operation, RegisterFrame& frame) {
    return lowerConstOperation<IR::Operations::ConstBooleanOperation>(operation, frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerConstFloat(const std::shared_ptr<IR::Operations::ConstFloatOperation>& operation, RegisterFrame& frame) {
    return lowerConstOperation<IR::Operations::ConstFloatOperation>(operation, frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerConstInt(const std::shared_ptr<IR::Operations::ConstIntOperation>& operation, RegisterFrame& frame) {
    return lowerConstOperation<IR::Operations::ConstIntOperation>(operation, frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerDiv(const std::shared_ptr<IR::Operations::DivOperation>& operation, RegisterFrame& frame) {
    return lowerBinaryOperation<IR::Operations::DivOperation>(operation, "/", frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerFunction(const std::shared_ptr<IR::Operations::FunctionOperation>&, RegisterFrame&) {
    NES_NOT_IMPLEMENTED();
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerIf(const std::shared_ptr<IR::Operations::IfOperation>& operation, RegisterFrame& frame) {
    auto trueBlockInvocation = std::make_shared<IR::Operations::BasicBlockInvocation>(operation->getTrueBlockInvocation());
    auto loweredTrueInvocation = lowerBasicBlockInvocation(trueBlockInvocation, frame);
    auto trueBlock = trueBlockInvocation->getBlock();
    auto loweredTrueBlock = lowerBasicBlock(trueBlock, frame);
    auto loweredTrueBlockLabel = loweredTrueBlock->getLabel();
    auto ifStmt = std::make_shared<CodeGen::Segment>();
    auto ifGoto = std::make_shared<CodeGen::CPP::Statement>("goto " + loweredTrueBlockLabel);
    if (activeBlocks.find(loweredTrueBlock->getLabel()) == activeBlocks.end()) {
        functionBody->add(loweredTrueBlock);
    }
    ifStmt->add(std::move(loweredTrueInvocation));
    ifStmt->add(ifGoto);

    auto falseBlockInvocation = std::make_shared<IR::Operations::BasicBlockInvocation>(operation->getFalseBlockInvocation());
    auto loweredFalseInvocation = lowerBasicBlockInvocation(falseBlockInvocation, frame);
    auto falseBlock = falseBlockInvocation->getBlock();
    auto loweredFalseBlock = lowerBasicBlock(falseBlock, frame);
    auto loweredFalseBlockLabel = loweredFalseBlock->getLabel();
    auto elseStmt = std::make_shared<CodeGen::Segment>();
    auto elseGoto = std::make_shared<CodeGen::CPP::Statement>("goto " + loweredFalseBlockLabel);
    if (activeBlocks.find(loweredFalseBlock->getLabel()) == activeBlocks.end()) {
        functionBody->add(loweredFalseBlock);
    }
    elseStmt->add(std::move(loweredFalseInvocation));
    elseStmt->add(elseGoto);

    auto conditionalReg = frame.getValue(operation->getValue()->getIdentifier());
    return std::make_unique<CodeGen::CPP::IfElseSegment>(conditionalReg, ifStmt, elseStmt);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerMod(const std::shared_ptr<IR::Operations::ModOperation>& operation, RegisterFrame& frame) {
    return lowerBinaryOperation<IR::Operations::ModOperation>(operation, "%", frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerMul(const std::shared_ptr<IR::Operations::MulOperation>& operation, RegisterFrame& frame) {
    return lowerBinaryOperation<IR::Operations::MulOperation>(operation, "*", frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerNegate(const std::shared_ptr<IR::Operations::NegateOperation>& operation, RegisterFrame& frame) {
    auto input = frame.getValue(operation->getInput()->getIdentifier());
    auto var = getVariable(operation->getIdentifier());
    frame.setValue(operation->getIdentifier(), var);
    auto targetType = getType(operation->getStamp());
    auto code = std::make_unique<CodeGen::Segment>();
    auto decl = std::make_shared<CodeGen::CPP::Statement>(targetType + " " + var);
    code->addToProlog(decl);
    auto stmt = std::make_shared<CodeGen::CPP::Statement>(var + " = !" + input);
    code->add(stmt);
    return std::move(code);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerOr(const std::shared_ptr<IR::Operations::OrOperation>& operation, RegisterFrame& frame) {
    return lowerBinaryOperation<IR::Operations::OrOperation>(operation, "||", frame);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerLoad(const std::shared_ptr<IR::Operations::LoadOperation>& operation, RegisterFrame& frame) {
    auto address = frame.getValue(operation->getAddress()->getIdentifier());
    auto var = getVariable(operation->getIdentifier());
    frame.setValue(operation->getIdentifier(), var);
    auto type = getType(operation->getStamp());
    auto code = std::make_unique<CodeGen::Segment>();
    auto decl = std::make_shared<CodeGen::CPP::Statement>(type + " " + var);
    code->addToProlog(decl);
    auto stmt = std::make_shared<CodeGen::CPP::Statement>(var + " = *reinterpret_cast<" + type + "*>(" + address + ")");
    code->add(stmt);
    return std::move(code);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerLoop(const std::shared_ptr<IR::Operations::LoopOperation>&, RegisterFrame&) {
    NES_NOT_IMPLEMENTED();
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerProxyCall(const std::shared_ptr<IR::Operations::ProxyCallOperation>& operation, RegisterFrame& frame) {
    std::string argTypes;
    std::string args;
    auto inputArguments = operation->getInputArguments();
    for (size_t i = 0; i < inputArguments.size(); i++) {
        auto arg = inputArguments[i];
        if (i != 0) {
            argTypes += ",";
            args += ",";
        }
        args += frame.getValue(arg->getIdentifier());
        argTypes += getType(arg->getStamp());
    }

    auto functionName = operation->getFunctionSymbol();
    auto returnType = getType(operation->getStamp());
    auto functionSignature = fmt::format("auto {} = ({}(*)({})) {}", functionName, returnType, argTypes, fmt::ptr(operation->getFunctionPtr()));

    auto code = std::make_unique<CodeGen::Segment>();
    auto functionDecl = std::make_shared<CodeGen::CPP::Statement>(functionSignature);
    code->addToProlog(functionDecl);

    std::shared_ptr<CodeGen::CPP::Statement> stmt;
    if (!operation->getStamp()->isVoid()) {
        auto var = getVariable(operation->getIdentifier());
        auto type = getType(operation->getStamp());
        frame.setValue(operation->getIdentifier(), var);
        auto decl = std::make_shared<CodeGen::CPP::Statement>(type + " " + var);
        code->addToProlog(decl);
        stmt = std::make_shared<CodeGen::CPP::Statement>(var + " = " + functionName + "(" + args + ")");
    } else {
        stmt = std::make_shared<CodeGen::CPP::Statement>(functionName + "(" + args + ")");
    }
    code->add(stmt);
    return std::move(code);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerReturn(const std::shared_ptr<IR::Operations::ReturnOperation>& operation, RegisterFrame& frame) {
    auto returnOpt = std::static_pointer_cast<IR::Operations::ReturnOperation>(operation);
    std::shared_ptr<CodeGen::CPP::Statement> stmt;
    if (returnOpt->hasReturnValue()) {
        auto returnFOp = frame.getValue(returnOpt->getReturnValue()->getIdentifier());
        stmt = std::make_shared<CodeGen::CPP::Statement>("return " + returnFOp);
    } else {
        stmt = std::make_shared<CodeGen::CPP::Statement>("return");
    }
    auto code = std::make_unique<CodeGen::Segment>();
    code->add(stmt);
    return std::move(code);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerStore(const std::shared_ptr<IR::Operations::StoreOperation>& operation, RegisterFrame& frame) {
    auto address = frame.getValue(operation->getAddress()->getIdentifier());
    auto value = frame.getValue(operation->getValue()->getIdentifier());
    auto type = getType(operation->getValue()->getStamp());
    auto stmt = std::make_shared<CodeGen::CPP::Statement>("*reinterpret_cast< " + type + "*>(" + address + ") = " + value);
    auto code = std::make_unique<CodeGen::Segment>();
    code->add(stmt);
    return std::move(code);
}

CPPLoweringInterface::Code CPPLoweringInterface::lowerSub(const std::shared_ptr<IR::Operations::SubOperation>& operation, RegisterFrame& frame) {
    return lowerBinaryOperation<IR::Operations::SubOperation>(operation, "-", frame);
}

} // namespace NES::Nautilus::Backends::CPP
