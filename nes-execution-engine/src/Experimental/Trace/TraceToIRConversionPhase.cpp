#include <Experimental/Trace/TraceToIRConversionPhase.hpp>

namespace NES::ExecutionEngine::Experimental::Trace {

std::shared_ptr<IR::NESIR> TraceToIRConversionPhase::apply(std::shared_ptr<ExecutionTrace> trace) {
    auto phaseContext = IRConversionContext(std::move(trace));
    return phaseContext.process();
};

std::shared_ptr<IR::NESIR> TraceToIRConversionPhase::IRConversionContext::process()  {
    auto& rootBlock = trace->getBlocks().front();
    auto rootIrBlock = processBlock(0, rootBlock);
    auto functionOperation = std::make_shared<IR::Operations::FunctionOperation>(
        "execute",
        /*argumentTypes*/ std::vector<IR::Operations::Operation::BasicType>{},
        /*arguments*/ std::vector<std::string>{},
        IR::Operations::Operation::INT64);
    functionOperation->addFunctionBasicBlock(rootIrBlock);
    ir->addRootOperation(functionOperation);
    return ir;
}

IR::BasicBlockPtr TraceToIRConversionPhase::IRConversionContext::processBlock(int32_t scope, Block& block) {
    std::vector<std::string> blockArgumentIdentifiers;
    std::vector<IR::Operations::Operation::BasicType> blockArgumentTypes;
    for (auto& arg : block.arguments) {
        blockArgumentIdentifiers.emplace_back(createValueIdentifier(arg));
        blockArgumentTypes.emplace_back(IR::Operations::Operation::BasicType::INT64);
    }
    IR::BasicBlockPtr irBasicBlock =
        std::make_shared<IR::BasicBlock>(std::to_string(block.blockId),
                                         scope,
                                         /*operations*/ std::vector<IR::Operations::OperationPtr>{},
                                         /*arguments*/ blockArgumentIdentifiers,
                                         /*argumentTypes*/ blockArgumentTypes);
    for (auto& operation : block.operations) {
        processOperation(scope, irBasicBlock, operation);
    }
    return irBasicBlock;
}

void TraceToIRConversionPhase::IRConversionContext::processOperation(int32_t scope, IR::BasicBlockPtr& currentBlock, Operation& operation) {

    switch (operation.op) {
        case ADD: {
            auto constOperation =
                std::make_shared<IR::Operations::AddIntOperation>(createValueIdentifier(operation.result),
                                                                  createValueIdentifier(operation.input[0]),
                                                                  createValueIdentifier(operation.input[1]));
            currentBlock->addOperation(constOperation);
        };
        case SUB: break;
        case DIV: break;
        case MUL: break;
        case EQUALS: break;
        case LESS_THAN: break;
        case NEGATE: break;
        case AND: break;
        case OR: break;
        case CMP: {
            processCMP(scope, currentBlock, operation);
            return;
        };
        case JMP: {
            processJMP(scope, currentBlock, operation);
            return;
        };
        case CONST: {
            auto valueRef = get<ConstantValue>(operation.input[0]);
            // TODO check data type
            auto intValue = std::static_pointer_cast<Interpreter::Integer>(valueRef.value);
            auto constOperation =
                std::make_shared<IR::Operations::ConstantIntOperation>(createValueIdentifier(operation.result), intValue->value, 64);
            currentBlock->addOperation(constOperation);
            return;
        };
        case ASSIGN: break;
        case RETURN: {
            auto operation = std::make_shared<IR::Operations::ReturnOperation>(0);
            currentBlock->addOperation(operation);
            return;
        };
        case LOAD: break;
        case STORE: break;
        case CALL: break;
    }
    //  NES_NOT_IMPLEMENTED();
}

void TraceToIRConversionPhase::IRConversionContext::processJMP(int32_t scope, IR::BasicBlockPtr& block, Operation& operation) {
    std::cout << "current block " << operation << std::endl;
    auto blockRef = get<BlockRef>(operation.input[0]);
    auto branchOperation = std::make_shared<IR::Operations::BranchOperation>(createBlockArguments(blockRef));
    if (blockMap.contains(blockRef.block)) {
        branchOperation->setNextBlock(blockMap[blockRef.block]);
    } else {
        auto targetBlock = processBlock(scope - 1, trace->getBlock(blockRef.block));
        blockMap[blockRef.block] = targetBlock;
        branchOperation->setNextBlock(targetBlock);
    }
    block->addOperation(branchOperation);
}

void TraceToIRConversionPhase::IRConversionContext::processCMP(int32_t scope, IR::BasicBlockPtr& currentBlock, Operation& operation) {

    auto valueRef = get<ValueRef>(operation.result);
    auto trueCaseBlockRef = get<BlockRef>(operation.input[0]);
    auto falseCaseBlockRef = get<BlockRef>(operation.input[1]);

    auto ifOperation = std::make_shared<IR::Operations::IfOperation>(createValueIdentifier(valueRef),
                                                                     createBlockArguments(trueCaseBlockRef),
                                                                     createBlockArguments(falseCaseBlockRef));
    auto trueCaseBlock = processBlock(scope + 1, trace->getBlock(trueCaseBlockRef.block));
    ifOperation->setThenBranchBlock(trueCaseBlock);
    auto falseCaseBlock = processBlock(scope + 1, trace->getBlock(falseCaseBlockRef.block));
    ifOperation->setElseBranchBlock(falseCaseBlock);
    currentBlock->addOperation(ifOperation);
}

std::vector<std::string> TraceToIRConversionPhase::IRConversionContext::createBlockArguments(BlockRef val)  {
    std::vector<std::string> blockArgumentIdentifiers;
    for (auto& arg : val.arguments) {
        blockArgumentIdentifiers.emplace_back(createValueIdentifier(arg));
    }
    return blockArgumentIdentifiers;
}

std::string TraceToIRConversionPhase::IRConversionContext::createValueIdentifier(InputVariant val) {
    auto valueRef = get<ValueRef>(val);
    return std::to_string(valueRef.operationId) + "_" + std::to_string(valueRef.blockId);
}


}