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

#include <Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/ModOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Nautilus/IR/Operations/CastOperation.hpp>
#include <Nautilus/IR/Operations/ConstBooleanOperation.hpp>
#include <Nautilus/IR/Operations/LoadOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseAndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseLeftShiftOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseOrOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseRightShiftOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseXorOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopInfo.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/ProxyCallOperation.hpp>
#include <Nautilus/IR/Operations/StoreOperation.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/Interface/DataTypes/Float/Double.hpp>
#include <Nautilus/Interface/DataTypes/Float/Float.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <vector>

namespace NES::Nautilus::Tracing {

std::shared_ptr<NES::Nautilus::IR::IRGraph> TraceToIRConversionPhase::apply(std::shared_ptr<ExecutionTrace> trace) {
    auto phaseContext = IRConversionContext(std::move(trace));
    return phaseContext.process();
};

std::shared_ptr<NES::Nautilus::IR::IRGraph> TraceToIRConversionPhase::IRConversionContext::process() {
    auto& rootBlock = trace->getBlocks().front();
    auto rootIrBlock = processBlock(0, rootBlock);

    auto& returnOperation = trace->getBlock(trace->getReturn()->blockId).operations.back();
    auto returnType = std::get<ValueRef>(returnOperation.result).type;
    auto intV = cast<NES::Nautilus::IR::Types::IntegerStamp>(returnType);
    auto functionOperation = std::make_shared<NES::Nautilus::IR::Operations::FunctionOperation>(
        "execute",
        /*argumentTypes*/ std::vector<NES::Nautilus::IR::Operations::PrimitiveStamp>{},
        /*arguments*/ std::vector<std::string>{},
        returnType);
    functionOperation->addFunctionBasicBlock(rootIrBlock);
    ir->addRootOperation(functionOperation);
    return ir;
}

NES::Nautilus::IR::BasicBlockPtr TraceToIRConversionPhase::IRConversionContext::processBlock(int32_t scope, Block& block) {
    // create new frame and block
    ValueFrame blockFrame;
    std::vector<std::shared_ptr<NES::Nautilus::IR::Operations::BasicBlockArgument>> blockArguments;
    for (auto& arg : block.arguments) {
        auto argumentIdentifier = createValueIdentifier(arg);
        auto blockArgument = std::make_shared<NES::Nautilus::IR::Operations::BasicBlockArgument>(argumentIdentifier, arg.type);
        blockArguments.emplace_back(blockArgument);
        blockFrame.setValue(argumentIdentifier, blockArgument);
    }

    NES::Nautilus::IR::BasicBlockPtr irBasicBlock =
        std::make_shared<NES::Nautilus::IR::BasicBlock>(std::to_string(block.blockId),
                                                        scope,
                                                        /*operations*/ std::vector<NES::Nautilus::IR::Operations::OperationPtr>{},
                                                        /*arguments*/ blockArguments);
    blockMap[block.blockId] = irBasicBlock;
    for (auto& operation : block.operations) {
        processOperation(scope, blockFrame, block, irBasicBlock, operation);
    }
    return irBasicBlock;
}

void TraceToIRConversionPhase::IRConversionContext::processOperation(int32_t scope,
                                                                     ValueFrame& frame,
                                                                     Block& currentBlock,
                                                                     NES::Nautilus::IR::BasicBlockPtr& currentIrBlock,
                                                                     TraceOperation& operation) {

    switch (operation.op) {
        case OpCode::ADD: {
            processAdd(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::SUB: {
            processSub(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::DIV: {
            processDiv(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::MOD: {
            processMod(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::MUL: {
            processMul(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::EQUALS: {
            processEquals(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::LESS_THAN: {
            processLessThan(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::GREATER_THAN: {
            processGreaterThan(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::NEGATE: {
            processNegate(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::BITWISE_AND: {
            processBitWiseAnd(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::BITWISE_OR: {
            processBitWiseOr(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::BITWISE_XOR: {
            processBitWiseXor(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::BITWISE_LEFT_SHIFT: {
            processBitWiseLeftShift(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::BITWISE_RIGHT_SHIFT: {
            processBitWiseRightShift(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::AND: {
            processAnd(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::OR: {
            processOr(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::CMP: {
            processCMP(scope, frame, currentBlock, currentIrBlock, operation);
            return;
        };
        case OpCode::JMP: {
            processJMP(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::CONST: {
            processConst(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::ASSIGN: break;
        case OpCode::RETURN: {
            if (std::get<ValueRef>(operation.result).type->isVoid()) {
                auto operation = std::make_shared<NES::Nautilus::IR::Operations::ReturnOperation>();
                currentIrBlock->addOperation(operation);
            } else {
                auto returnValue = frame.getValue(createValueIdentifier(operation.input[0]));
                auto operation = std::make_shared<NES::Nautilus::IR::Operations::ReturnOperation>(returnValue);
                currentIrBlock->addOperation(operation);
            }

            return;
        };
        case OpCode::LOAD: {
            processLoad(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::STORE: {
            processStore(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::CAST: {
            processCast(scope, frame, currentIrBlock, operation);
            return;
        };
        case OpCode::CALL: processCall(scope, frame, currentIrBlock, operation); return;
    }
    //  NES_NOT_IMPLEMENTED();
}

void TraceToIRConversionPhase::IRConversionContext::processJMP(int32_t scope,
                                                               ValueFrame& frame,
                                                               NES::Nautilus::IR::BasicBlockPtr& block,
                                                               TraceOperation& operation) {
    std::stringstream operationAsString;
    operationAsString << operation;
    NES_DEBUG("current block {} {}", block->getIdentifier(), operationAsString.str());
    auto blockRef = get<BlockRef>(operation.input[0]);
    NES::Nautilus::IR::Operations::BasicBlockInvocation blockInvocation;
    createBlockArguments(frame, blockInvocation, blockRef);

    if (blockMap.contains(blockRef.block)) {
        block->addNextBlock(blockMap[blockRef.block], blockInvocation.getArguments());
        return;
    }
    auto targetBlock = trace->getBlock(blockRef.block);
    // Problem:
    // trueCaseBlock = trace->getBlock(get<BlockRef>(operation.input[0]))
    // targetBlock   = get<BlockRef>(operation.input[0])

    // check if we jump to a loop head:
    if (targetBlock.operations.back().op == OpCode::CMP) {
        auto trueCaseBlockRef = get<BlockRef>(operation.input[0]);
    }

    auto resultTargetBlock = processBlock(scope - 1, trace->getBlock(blockRef.block));
    blockMap[blockRef.block] = resultTargetBlock;
    block->addNextBlock(resultTargetBlock, blockInvocation.getArguments());
}

void TraceToIRConversionPhase::IRConversionContext::processCMP(int32_t scope,
                                                               ValueFrame& frame,
                                                               Block&,
                                                               NES::Nautilus::IR::BasicBlockPtr& currentIrBlock,
                                                               TraceOperation& operation) {

    auto valueRef = get<ValueRef>(operation.result);
    auto trueCaseBlockRef = get<BlockRef>(operation.input[0]);
    auto falseCaseBlockRef = get<BlockRef>(operation.input[1]);

    auto booleanValue = frame.getValue(createValueIdentifier(valueRef));
    auto ifOperation = std::make_shared<NES::Nautilus::IR::Operations::IfOperation>(booleanValue);
    auto trueCaseBlock = processBlock(scope + 1, trace->getBlock(trueCaseBlockRef.block));

    ifOperation->getTrueBlockInvocation().setBlock(trueCaseBlock);
    createBlockArguments(frame, ifOperation->getTrueBlockInvocation(), trueCaseBlockRef);

    auto falseCaseBlock = processBlock(scope + 1, trace->getBlock(falseCaseBlockRef.block));
    ifOperation->getFalseBlockInvocation().setBlock(falseCaseBlock);
    createBlockArguments(frame, ifOperation->getFalseBlockInvocation(), falseCaseBlockRef);
    currentIrBlock->addOperation(ifOperation);
}

std::vector<std::string> TraceToIRConversionPhase::IRConversionContext::createBlockArguments(BlockRef val) {
    std::vector<std::string> blockArgumentIdentifiers;
    for (auto& arg : val.arguments) {
        blockArgumentIdentifiers.emplace_back(createValueIdentifier(arg));
    }
    return blockArgumentIdentifiers;
}

void TraceToIRConversionPhase::IRConversionContext::createBlockArguments(
    ValueFrame& frame,
    NES::Nautilus::IR::Operations::BasicBlockInvocation& blockInvocation,
    BlockRef val) {
    for (auto& arg : val.arguments) {
        auto valueIdentifier = createValueIdentifier(arg);
        blockInvocation.addArgument(frame.getValue(valueIdentifier));
    }
}

std::string TraceToIRConversionPhase::IRConversionContext::createValueIdentifier(InputVariant val) {
    if (holds_alternative<ValueRef>(val)) {
        auto valueRef = std::get<ValueRef>(val);
        return std::to_string(valueRef.blockId) + "_" + std::to_string(valueRef.operationId);
    } else
        return "";
}

void TraceToIRConversionPhase::IRConversionContext::processAdd(int32_t,
                                                               ValueFrame& frame,
                                                               NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                               TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto addOperation = std::make_shared<NES::Nautilus::IR::Operations::AddOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, addOperation);
    currentBlock->addOperation(addOperation);
}

void TraceToIRConversionPhase::IRConversionContext::processSub(int32_t,
                                                               ValueFrame& frame,
                                                               NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                               TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto subOperation = std::make_shared<NES::Nautilus::IR::Operations::SubOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, subOperation);
    currentBlock->addOperation(subOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processMul(int32_t,
                                                               ValueFrame& frame,
                                                               NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                               TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto mulOperation = std::make_shared<NES::Nautilus::IR::Operations::MulOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, mulOperation);
    currentBlock->addOperation(mulOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processDiv(int32_t,
                                                               ValueFrame& frame,
                                                               NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                               TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto divOperation = std::make_shared<NES::Nautilus::IR::Operations::DivOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, divOperation);
    currentBlock->addOperation(divOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processMod(int32_t,
                                                               ValueFrame& frame,
                                                               NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                               TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto modOperation = std::make_shared<NES::Nautilus::IR::Operations::ModOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, modOperation);
    currentBlock->addOperation(modOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processNegate(int32_t,
                                                                  ValueFrame& frame,
                                                                  NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                                  TraceOperation& operation) {
    auto input = frame.getValue(createValueIdentifier(operation.input[0]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto negateOperation = std::make_shared<NES::Nautilus::IR::Operations::NegateOperation>(resultIdentifier, input);
    frame.setValue(resultIdentifier, negateOperation);
    currentBlock->addOperation(negateOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processLessThan(int32_t,
                                                                    ValueFrame& frame,
                                                                    NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                                    TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));

    NES::Nautilus::IR::Operations::CompareOperation::Comparator comparator;
    comparator = NES::Nautilus::IR::Operations::CompareOperation::Comparator::LT;

    auto resultIdentifier = createValueIdentifier(operation.result);
    auto compareOperation = std::make_shared<NES::Nautilus::IR::Operations::CompareOperation>(
        resultIdentifier,
        leftInput,
        rightInput,
        NES::Nautilus::IR::Operations::CompareOperation::Comparator::LT);
    frame.setValue(resultIdentifier, compareOperation);
    currentBlock->addOperation(compareOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processGreaterThan(int32_t,
                                                                       ValueFrame& frame,
                                                                       NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                                       TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));

    NES::Nautilus::IR::Operations::CompareOperation::Comparator comparator;
    comparator = NES::Nautilus::IR::Operations::CompareOperation::Comparator::GT;

    auto resultIdentifier = createValueIdentifier(operation.result);
    auto compareOperation = std::make_shared<NES::Nautilus::IR::Operations::CompareOperation>(
        resultIdentifier,
        leftInput,
        rightInput,
        NES::Nautilus::IR::Operations::CompareOperation::Comparator::GT);
    frame.setValue(resultIdentifier, compareOperation);
    currentBlock->addOperation(compareOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processEquals(int32_t,
                                                                  ValueFrame& frame,
                                                                  NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                                  TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto compareOperation = std::make_shared<NES::Nautilus::IR::Operations::CompareOperation>(
        resultIdentifier,
        leftInput,
        rightInput,
        NES::Nautilus::IR::Operations::CompareOperation::Comparator::EQ);
    frame.setValue(resultIdentifier, compareOperation);
    currentBlock->addOperation(compareOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processAnd(int32_t,
                                                               ValueFrame& frame,
                                                               NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                               TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto andOperation = std::make_shared<NES::Nautilus::IR::Operations::AndOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, andOperation);
    currentBlock->addOperation(andOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processOr(int32_t,
                                                              ValueFrame& frame,
                                                              NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                              TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto orOperation = std::make_shared<NES::Nautilus::IR::Operations::OrOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, orOperation);
    currentBlock->addOperation(orOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processBitWiseAnd(int32_t,
                                                                      ValueFrame& frame,
                                                                      NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                                      TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto orOperation =
        std::make_shared<NES::Nautilus::IR::Operations::BitWiseAndOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, orOperation);
    currentBlock->addOperation(orOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processBitWiseOr(int32_t,
                                                                     ValueFrame& frame,
                                                                     NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                                     TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto orOperation =
        std::make_shared<NES::Nautilus::IR::Operations::BitWiseOrOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, orOperation);
    currentBlock->addOperation(orOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processBitWiseXor(int32_t,
                                                                      ValueFrame& frame,
                                                                      NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                                      TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto orOperation =
        std::make_shared<NES::Nautilus::IR::Operations::BitWiseXorOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, orOperation);
    currentBlock->addOperation(orOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processBitWiseLeftShift(int32_t,
                                                                            ValueFrame& frame,
                                                                            NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                                            TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto orOperation =
        std::make_shared<NES::Nautilus::IR::Operations::BitWiseLeftShiftOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, orOperation);
    currentBlock->addOperation(orOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processBitWiseRightShift(int32_t,
                                                                             ValueFrame& frame,
                                                                             NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                                             TraceOperation& operation) {
    auto leftInput = frame.getValue(createValueIdentifier(operation.input[0]));
    auto rightInput = frame.getValue(createValueIdentifier(operation.input[1]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto orOperation =
        std::make_shared<NES::Nautilus::IR::Operations::BitWiseRightShiftOperation>(resultIdentifier, leftInput, rightInput);
    frame.setValue(resultIdentifier, orOperation);
    currentBlock->addOperation(orOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processLoad(int32_t,
                                                                ValueFrame& frame,
                                                                NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                                TraceOperation& operation) {
    // TODO add load data type
    //auto constOperation = std::make_shared<NES::Nautilus::IR::Operations::LoadOperation>(createValueIdentifier(operation.result),
    //                                                                      createValueIdentifier(operation.input[0]),
    //                                                                      NES::Nautilus::IR::Operations::Operation::BasicType::VOID);
    //currentBlock->addOperation(constOperation);
    auto address = frame.getValue(createValueIdentifier(operation.input[0]));
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto resultType = std::get<ValueRef>(operation.result).type;
    auto loadOperation = std::make_shared<NES::Nautilus::IR::Operations::LoadOperation>(resultIdentifier, address, resultType);
    frame.setValue(resultIdentifier, loadOperation);
    currentBlock->addOperation(loadOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processStore(int32_t,
                                                                 ValueFrame& frame,
                                                                 NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                                 TraceOperation& operation) {
    auto address = frame.getValue(createValueIdentifier(operation.input[1]));
    auto value = frame.getValue(createValueIdentifier(operation.input[0]));
    auto storeOperation = std::make_shared<NES::Nautilus::IR::Operations::StoreOperation>(address, value);
    currentBlock->addOperation(storeOperation);
}

void TraceToIRConversionPhase::IRConversionContext::processCall(int32_t,
                                                                ValueFrame& frame,
                                                                NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                                TraceOperation& operation) {

    auto inputArguments = std::vector<NES::Nautilus::IR::Operations::OperationWPtr>{};
    auto functionCallTarget = std::get<FunctionCallTarget>(operation.input[0]);

    for (uint32_t i = 1; i < operation.input.size(); i++) {
        auto input = frame.getValue(createValueIdentifier(operation.input[i]));
        inputArguments.emplace_back(input);
    }

    auto resultType = std::holds_alternative<None>(operation.result) ? NES::Nautilus::IR::Types::StampFactory::createVoidStamp()
                                                                     : std::get<ValueRef>(operation.result).type;
    auto resultIdentifier = createValueIdentifier(operation.result);
    auto proxyCallOperation = std::make_shared<NES::Nautilus::IR::Operations::ProxyCallOperation>(
        NES::Nautilus::IR::Operations::ProxyCallOperation::ProxyCallType::Other,
        functionCallTarget.mangledName,
        functionCallTarget.functionPtr,
        resultIdentifier,
        inputArguments,
        resultType);
    if (!resultType->isVoid()) {
        frame.setValue(resultIdentifier, proxyCallOperation);
    }
    currentBlock->addOperation(proxyCallOperation);
}

bool TraceToIRConversionPhase::IRConversionContext::isBlockInLoop(uint32_t parentBlockId, uint32_t currentBlockId) {
    if (currentBlockId == parentBlockId) {
        return true;
    }
    if (parentBlockId == 8)
        return false;
    if (currentBlockId == UINT32_MAX) {
        currentBlockId = parentBlockId;
    }
    auto currentBlock = trace->getBlock(currentBlockId);
    auto& terminationOp = currentBlock.operations.back();
    if (terminationOp.op == OpCode::CMP) {
        auto trueCaseBlockRef = get<BlockRef>(terminationOp.input[0]);
        auto falseCaseBlockRef = get<BlockRef>(terminationOp.input[1]);
        return currentBlock.type == Block::Type::ControlFlowMerge;
        //isBlockInLoop(parentBlockId, trueCaseBlockRef.block) || isBlockInLoop(parentBlockId, falseCaseBlockRef.block);
    } else if (terminationOp.op == OpCode::JMP) {
        auto target = get<BlockRef>(terminationOp.input[0]);
        return isBlockInLoop(parentBlockId, target.block);
    }
    return false;
}
void TraceToIRConversionPhase::IRConversionContext::processConst(int32_t,
                                                                 TraceToIRConversionPhase::ValueFrame& frame,
                                                                 NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                                 TraceOperation& operation) {
    auto valueRef = get<ConstantValue>(operation.input[0]);
    auto resultIdentifier = createValueIdentifier(operation.result);
    NES::Nautilus::IR::Operations::OperationPtr constOperation;
    if (auto* i8 = cast_if<Int8>(valueRef.value.get())) {
        constOperation = std::make_shared<IR::Operations::ConstIntOperation>(resultIdentifier, i8->getValue(), i8->getType());
    } else if (auto* i16 = cast_if<Int16>(valueRef.value.get())) {
        constOperation = std::make_shared<IR::Operations::ConstIntOperation>(resultIdentifier, i16->getValue(), i16->getType());
    } else if (auto* i32 = cast_if<Int32>(valueRef.value.get())) {
        constOperation = std::make_shared<IR::Operations::ConstIntOperation>(resultIdentifier, i32->getValue(), i32->getType());
    } else if (auto* i64 = cast_if<Int64>(valueRef.value.get())) {
        constOperation = std::make_shared<IR::Operations::ConstIntOperation>(resultIdentifier, i64->getValue(), i64->getType());
    } else if (auto* ui8 = cast_if<UInt8>(valueRef.value.get())) {
        constOperation = std::make_shared<IR::Operations::ConstIntOperation>(resultIdentifier, ui8->getValue(), ui8->getType());
    } else if (auto* ui16 = cast_if<UInt16>(valueRef.value.get())) {
        constOperation = std::make_shared<IR::Operations::ConstIntOperation>(resultIdentifier, ui16->getValue(), ui16->getType());
    } else if (auto* ui32 = cast_if<UInt32>(valueRef.value.get())) {
        constOperation = std::make_shared<IR::Operations::ConstIntOperation>(resultIdentifier, ui32->getValue(), ui32->getType());
    } else if (auto* ui64 = cast_if<UInt64>(valueRef.value.get())) {
        constOperation = std::make_shared<IR::Operations::ConstIntOperation>(resultIdentifier, ui64->getValue(), ui64->getType());
    } else if (auto* float32 = cast_if<Float>(valueRef.value.get())) {
        constOperation = std::make_shared<NES::Nautilus::IR::Operations::ConstFloatOperation>(resultIdentifier,
                                                                                              float32->getValue(),
                                                                                              float32->getType());
    } else if (auto* float64 = cast_if<Double>(valueRef.value.get())) {
        constOperation = std::make_shared<NES::Nautilus::IR::Operations::ConstFloatOperation>(resultIdentifier,
                                                                                              float64->getValue(),
                                                                                              float64->getType());
    } else if (auto* boolean = cast_if<Boolean>(valueRef.value.get())) {
        constOperation =
            std::make_shared<NES::Nautilus::IR::Operations::ConstBooleanOperation>(resultIdentifier, boolean->getValue());
    } else {
        NES_THROW_RUNTIME_ERROR("Can't create const for value");
    }

    currentBlock->addOperation(constOperation);
    frame.setValue(resultIdentifier, constOperation);
}
void TraceToIRConversionPhase::IRConversionContext::processCast(int32_t,
                                                                TraceToIRConversionPhase::ValueFrame& frame,
                                                                NES::Nautilus::IR::BasicBlockPtr& currentBlock,
                                                                TraceOperation& operation) {

    auto resultIdentifier = createValueIdentifier(operation.result);
    auto input = frame.getValue(createValueIdentifier(operation.input[0]));
    auto resultType = std::get<ValueRef>(operation.result).type;
    auto castOperation = std::make_shared<NES::Nautilus::IR::Operations::CastOperation>(resultIdentifier, input, resultType);
    currentBlock->addOperation(castOperation);
    frame.setValue(resultIdentifier, castOperation);
}

}// namespace NES::Nautilus::Tracing