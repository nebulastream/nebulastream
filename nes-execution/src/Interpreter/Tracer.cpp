#include <Interpreter/DataValue/Boolean.hpp>
#include <Interpreter/DataValue/Value.hpp>
#include <Interpreter/Tracer.hpp>
#include <Interpreter/Util/Casting.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <execinfo.h>
#include <iostream>
#include <map>
namespace NES::Interpreter {

Tag TraceContext::createTag() {
    void* buffer[20];
    // First add the RIP pointers
    int size = backtrace(buffer, 20);
    std::vector<uint64_t> addresses;
    for (int i = 0; i < size; i++) {
        auto address = (uint64_t) buffer[i];
        if (address == startAddress) {
            size = i;
            break;
        }
    }

    for (int i = 0; i < size - 1; i++) {
        auto address = (uint64_t) buffer[i];
        addresses.push_back(address);
    }
    char** backtrace_functions = backtrace_symbols(buffer, size);
    int i;
    bool found = false;
    for (i = 0; i < size; i++) {
        unsigned int offset;
        unsigned long long address;
        printf("%s\n", backtrace_functions[i]);
    }
    return {addresses};
}

uint64_t TraceContext::createStartAddress() {
    void* buffer[20];
    // First add the RIP pointers
    int size = backtrace(buffer, 20);
    char** backtrace_functions = backtrace_symbols(buffer, size);
    int i;
    bool found = false;
    for (i = 0; i < size; i++) {
        unsigned int offset;
        unsigned long long address;
        printf("%s\n", backtrace_functions[i]);
        //if (sscanf(backtrace_functions[i], "%*[^+]+%x) [%llx]", &offset,
        //      &address) != 2) {
        // printf("Scanning of backtrace failed, result might be
        // bad\n");
        //  continue;
        // }
    }
    return (uint64_t) buffer[5];
}

void ExecutionTrace::checkInputReference(uint32_t currentBlockIndex, ValueRef inputReference, ValueRef currentInput) {
    auto& currentBlock = getBlock(currentBlockIndex);
    if (currentBlock.isLocalValueRef(currentInput)) {
        // the ref is defined locally
        return;
    }
    for (auto predecessorIds : currentBlock.predecessors) {
        auto& preBlock = getBlock(predecessorIds);
        if (preBlock.isLocalValueRef(currentInput)) {
            auto& arguments = currentBlock.arguments;
            for (auto& input : preBlock.operations.back().input) {
                if (auto* ref = get_if<BlockRef>(&input)) {
                    if (ref->block == currentBlockIndex) {
                        if (std::find(ref->arguments.begin(), ref->arguments.end(), currentInput) == ref->arguments.end()) {
                            ref->arguments.emplace_back(currentInput);
                        }
                    }
                }
            }
            if (std::find(arguments.begin(), arguments.end(), inputReference) == arguments.end()) {
                // value ref is an input
                arguments.push_back(inputReference);
            }
            // the ref is defined locally
            continue;
        } else if (preBlock.isLocalValueRef(inputReference)) {
            // the ref is defined locally
            auto& arguments = currentBlock.arguments;
            if (std::find(arguments.begin(), arguments.end(), inputReference) == arguments.end()) {
                // value ref is an input
                auto& jump = preBlock.operations[preBlock.operations.size() - 1].input[0];
                auto& blockRef = get<BlockRef>(jump);
                if (std::find(blockRef.arguments.begin(), blockRef.arguments.end(), currentInput) == blockRef.arguments.end()) {
                    blockRef.arguments.emplace_back(currentInput);
                }
            }
            continue;
        } else {
            checkInputReference(predecessorIds, inputReference, currentInput);
            if (preBlock.isLocalValueRef(inputReference)) {
                for (auto& input : preBlock.operations.back().input) {
                    if (auto* ref = get_if<BlockRef>(&input)) {
                        if (ref->block == currentBlockIndex) {
                            ref->arguments.emplace_back(inputReference);
                        }
                    }
                }
            }
        }
    }
}

void TraceContext::trace(OpCode op, const Value& leftInput, const Value& rightInput, Value& result) {
    if (executionTrace.getCurrentBlock().operations.size() > currentOperationCounter) {
        // we should contain this operation
        auto operation = executionTrace.getCurrentBlock().operations[currentOperationCounter];
        if (operation.op == op) {
            std::cout << "Trace: handle same instruction " << std::endl;
            std::cout << executionTrace << std::endl;
            if (auto* valueRef = get_if<ValueRef>(&operation.input[0])) {
                executionTrace.checkInputReference(executionTrace.getCurrentBlockIndex(), *valueRef, leftInput.ref);
            }
            std::cout << executionTrace << std::endl;
            if (auto* valueRef = get_if<ValueRef>(&operation.input[1])) {
                executionTrace.checkInputReference(executionTrace.getCurrentBlockIndex(), *valueRef, rightInput.ref);
            }
            std::cout << executionTrace << std::endl;
            currentOperationCounter++;
            result.ref = std::get<ValueRef>(operation.result);
            return;
        } else {
            std::cout << "Trace: error " << std::endl;
        }
    }
    auto tag = createTag();
    std::cout << "Trace: " << magic_enum::enum_name(op) << "; tag: " << tag << std::endl;
    if (executionTrace.tagMap.contains(tag)) {
        std::cout << "Control-flow merge" << result.value << std::endl;
        auto res = executionTrace.tagMap.find(tag);
        auto [blockId, opId] = res->second;
        // create new block
        auto& mergeBlock = executionTrace.processControlFlowMerge(blockId, opId);
        currentOperationCounter = mergeBlock.operations.size() - 1;
    } else if (executionTrace.localTagMap.contains(tag)) {
        std::cout << "Local Control-flow merge-> Loop" << result.value << std::endl;
        std::cout << "Control-flow merge" << result.value << std::endl;
        std::cout << executionTrace << std::endl;
        auto res = executionTrace.localTagMap.find(tag);
        auto [blockId, opId] = res->second;
        auto& mergeBlock = executionTrace.processControlFlowMerge(blockId, opId);
        auto mergeOperation = mergeBlock.operations.front();
        result.ref = std::get<ValueRef>(mergeOperation.result);
        std::cout << executionTrace << std::endl;
        currentOperationCounter = 1;

    } else {
        auto leftInputRef = executionTrace.findReference(leftInput.ref, leftInput.srcRef);
        auto rightInputRef = executionTrace.findReference(rightInput.ref, rightInput.srcRef);
        auto operation = Operation(op, result.ref, {leftInputRef, rightInputRef});
        executionTrace.addOperation(operation);
        executionTrace.localTagMap.emplace(
            std::make_pair(tag, std::make_tuple(executionTrace.getCurrentBlockIndex(), currentOperationCounter)));
        currentOperationCounter++;
    }
}

Block& ExecutionTrace::processControlFlowMerge(uint32_t blockIndex, uint32_t operationIndex) {
    // perform a control flow merge and merge the current block with operations in some other block.
    // create new block
    auto mergedBlockId = createBlock();
    auto& mergeBlock = getBlock(mergedBlockId);
    // move operation to new block
    auto& oldBlock = getBlock(blockIndex);
    // copy everything between opId and end;

    for (uint32_t opIndex = operationIndex; opIndex < oldBlock.operations.size(); opIndex++) {
        auto sourceOperation = oldBlock.operations[opIndex];
        mergeBlock.operations.emplace_back(sourceOperation);
    }

    // rename vars in new block
    // todo reduce complexity
    for (uint32_t opIndex = 0; opIndex < mergeBlock.operations.size(); opIndex++) {
        auto& operation = mergeBlock.operations[opIndex];
        if (auto* valRef = std::get_if<ValueRef>(&operation.result)) {
            for (uint32_t remainingIndex = opIndex; remainingIndex < mergeBlock.operations.size(); remainingIndex++) {
                auto& subOperation = mergeBlock.operations[remainingIndex];
                for (auto& input : subOperation.input) {
                    if (auto* inputRef = std::get_if<ValueRef>(&input)) {
                        if (inputRef->operator==(*valRef)) {
                            inputRef->blockId = mergedBlockId;
                        }
                    }
                }
            }
            valRef->blockId = mergedBlockId;
        }
    }

    auto oldBlockRef = BlockRef(mergedBlockId);
    for (uint32_t opIndex = 0; opIndex < mergeBlock.operations.size(); opIndex++) {
        auto& operation = mergeBlock.operations[opIndex];
        for (auto& input : operation.input) {
            if (auto* inputRef = std::get_if<ValueRef>(&input)) {
                if (inputRef->blockId != mergedBlockId) {
                    mergeBlock.arguments.emplace_back(*inputRef);
                    oldBlockRef.arguments.emplace_back(*inputRef);
                }
            }
        }
    }

    // remove content beyond opID
    oldBlock.operations.erase(oldBlock.operations.begin() + operationIndex, oldBlock.operations.end());
    oldBlock.operations.emplace_back(Operation(JMP, ValueRef(0, 0), {oldBlockRef}));
    addOperation(Operation(JMP, ValueRef(0, 0), {BlockRef(mergedBlockId)}));

    mergeBlock.predecessors.emplace_back(blockIndex);
    mergeBlock.predecessors.emplace_back(currentBlock);
    setCurrentBloc(mergedBlockId);

    //
    auto& lastMergeOperation = mergeBlock.operations[mergeBlock.operations.size() - 1];
    if (lastMergeOperation.op == CMP || lastMergeOperation.op == JMP) {
        for (auto& input : lastMergeOperation.input) {
            auto& blockRef = std::get<BlockRef>(input);
            auto& blockPredecessor = getBlock(blockRef.block).predecessors;
            std::replace(blockPredecessor.begin(), blockPredecessor.end(), blockIndex, mergedBlockId);
            std::replace(blockPredecessor.begin(), blockPredecessor.end(), currentBlock, mergedBlockId);
        }
    }

    return mergeBlock;
}

void TraceContext::trace(OpCode op, const Value& input, Value& result) {

    if (executionTrace.getCurrentBlock().operations.size() > currentOperationCounter) {
        // we should contain this operation
        auto operation = executionTrace.getCurrentBlock().operations[currentOperationCounter];
        // pull next operation;
        if (operation.op == JMP) {
            executionTrace.setCurrentBloc(std::get<BlockRef>(operation.input[0]).block);
            currentOperationCounter = 0;
            operation = executionTrace.getCurrentBlock().operations[currentOperationCounter];
        }
        if (operation.op == op) {
            std::cout << "Trace: handle same instruction " << std::endl;
            if (op == CMP) {
                if (cast<Boolean>(input.value)->value) {
                    executionTrace.setCurrentBloc(std::get<BlockRef>(operation.input[0]).block);
                    currentOperationCounter = 0;
                    return;
                } else {
                    executionTrace.setCurrentBloc(std::get<BlockRef>(operation.input[1]).block);
                    currentOperationCounter = 0;
                    return;
                }
            }
            result.ref = std::get<ValueRef>(operation.result);
            result.srcRef = std::get<ValueRef>(operation.result);
            currentOperationCounter++;
            return;
        } else {
            std::cout << "Trace: error " << std::endl;
        }
    }

    auto tag = createTag();
    if (executionTrace.localTagMap.contains(tag)) {
        std::cout << "Local Control-flow merge-> Loop" << result.value << std::endl;
        std::cout << "Control-flow merge" << result.value << std::endl;
        std::cout << executionTrace << std::endl;
        auto res = executionTrace.localTagMap.find(tag);
        auto [blockId, opId] = res->second;
        auto& mergeBlock = executionTrace.processControlFlowMerge(blockId, opId);
        auto mergeOperation = mergeBlock.operations.front();
        result.ref = std::get<ValueRef>(mergeOperation.result);
        std::cout << executionTrace << std::endl;
        currentOperationCounter = 1;

    } else if (executionTrace.tagMap.contains(tag) && op != CMP) {
        std::cout << "Control-flow merge" << result.value << std::endl;
        auto res = executionTrace.tagMap.find(tag);
        auto [blockId, opId] = res->second;
        auto& mergeBlock = executionTrace.processControlFlowMerge(blockId, opId);
        auto mergeOperation = mergeBlock.operations.front();
        result.ref = std::get<ValueRef>(mergeOperation.result);
        std::cout << "Control-flow merge" << result.value << std::endl;
        currentOperationCounter = 1;
    } else if (op == CMP) {
        std::cout << "Trace: handle control-flow split. Current case: " << result.value << std::endl;

        auto trueBlock = executionTrace.createBlock();
        auto falseBlock = executionTrace.createBlock();
        executionTrace.getBlock(trueBlock).predecessors.emplace_back(executionTrace.getCurrentBlockIndex());
        executionTrace.getBlock(falseBlock).predecessors.emplace_back(executionTrace.getCurrentBlockIndex());
        auto operation = Operation(op, result.ref, {BlockRef(trueBlock), BlockRef(falseBlock)});
        executionTrace.addOperation(operation);
        executionTrace.tagMap.emplace(
            std::make_pair(tag, std::make_tuple(executionTrace.getCurrentBlockIndex(), currentOperationCounter)));
        if (cast<Boolean>(input.value)->value) {
            executionTrace.setCurrentBloc(trueBlock);
            currentOperationCounter = 0;
        } else {
            executionTrace.setCurrentBloc(falseBlock);
            currentOperationCounter = 0;
        }
    } else {
        if (op == OpCode::CONST) {
            auto constValue = input.value->copy();
            auto operation = Operation(op, result.ref, {ConstantValue(std::move(constValue))});
            executionTrace.addOperation(operation);
            executionTrace.localTagMap.emplace(
                std::make_pair(tag, std::make_tuple(executionTrace.getCurrentBlockIndex(), currentOperationCounter)));
            currentOperationCounter++;
        } else {
            auto operation = Operation(op, result.ref, {input.ref});
            executionTrace.addOperation(operation);
            executionTrace.localTagMap.emplace(
                std::make_pair(tag, std::make_tuple(executionTrace.getCurrentBlockIndex(), currentOperationCounter)));
            currentOperationCounter++;
        }
    }
}

bool Block::isLocalValueRef(ValueRef& ref) {
    if (ref.blockId == blockId) {
        // this is a local ref
        return true;
    }
    return std::find(arguments.begin(), arguments.end(), ref) != arguments.end();
}

void Block::addArgument(ValueRef ref) {
    // only add ref to arguments if it not already exists
    if (std::find(arguments.begin(), arguments.end(), ref) == arguments.end()) {
        arguments.emplace_back(ref);
    }
}

ValueRef ExecutionTrace::findReference(ValueRef ref, ValueRef srcRef) {
    if (getCurrentBlock().isLocalValueRef(ref) || getCurrentBlock().isLocalValueRef(srcRef)) {
        // reference to local variable;
        return ref;
    }
    return ExecutionTrace::createBlockArgument(currentBlock, ref, srcRef);
}

ValueRef ExecutionTrace::createBlockArgument(uint32_t blockIndex, ValueRef ref, ValueRef srcRef) {
    auto& block = getBlock(blockIndex);
    for (auto predecessor : block.predecessors) {
        auto& predecessorBlock = getBlock(predecessor);
        if (predecessorBlock.isLocalValueRef(ref)) {
            // last operation is a JMP or a CMP
            auto& lastOperation = predecessorBlock.operations.back();
            for (auto& opInputs : lastOperation.input) {
                if (auto blockRef = get_if<BlockRef>(&opInputs)) {
                    if (blockRef->block == blockIndex) {
                        blockRef->arguments.emplace_back(ref);
                        // reuse ref name for the argument as it is a unique combination of block and op id.
                        block.addArgument(ref);
                    }
                }
            }
        } else {
            // we have to check the parents and pass the variable as an argument
            ValueRef parentRef = createBlockArgument(predecessor, ref, srcRef);
            auto& lastOperation = predecessorBlock.operations.back();
            for (auto& opInputs : lastOperation.input) {
                if (auto blockRef = get_if<BlockRef>(&opInputs)) {
                    if (blockRef->block == blockIndex) {
                        blockRef->arguments.emplace_back(ref);
                        block.addArgument(ref);
                    }
                }
            }
        }
    }
    return ref;
}

std::ostream& operator<<(std::ostream& os, const Tag& tag) {
    os << "addresses: [";
    for (auto address : tag.addresses) {
        os << address << ";";
    }
    os << "]";
    return os;
}

std::ostream& operator<<(std::ostream& os, const ValueRef& valueRef) {
    os << "$" << valueRef.blockId << "_" << valueRef.operationId;
    return os;
}
bool ValueRef::operator==(const ValueRef& rhs) const { return blockId == rhs.blockId && operationId == rhs.operationId; }
bool ValueRef::operator!=(const ValueRef& rhs) const { return !(rhs == *this); }

std::ostream& operator<<(std::ostream& os, const ConstantValue& valueRef) {
    os << "c" << valueRef.value;
    return os;
}

std::ostream& operator<<(std::ostream& os, const ExecutionTrace& executionTrace) {

    for (size_t i = 0; i < executionTrace.blocks.size(); i++) {
        os << "Block" << i << "";
        os << executionTrace.blocks[i];
    }
    return os;
}
void ExecutionTrace::traceAssignment(ValueRef srcRef, ValueRef valueRef) {
    auto& frame = getCurrentBlock().frame;
    frame[srcRef] = valueRef;
}

std::ostream& operator<<(std::ostream& os, const Block& block) {
    os << "(";
    for (size_t i = 0; i < block.arguments.size(); i++) {
        os << block.arguments[i] << ",";
    }
    os << ")\n";
    for (size_t i = 0; i < block.operations.size(); i++) {
        os << "\t" << block.operations[i] << "\n";
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const BlockRef& block) {
    os << "B" << block.block << "(";
    for (auto argument : block.arguments) {
        os << argument << ",";
    }

    os << ")";

    return os;
}

std::ostream& operator<<(std::ostream& os, const Operation& operation) {
    os << magic_enum::enum_name(operation.op) << "\t";
    if (auto ref = std::get_if<ValueRef>(&operation.result)) {
        os << *ref << "\t";
    } else if (auto ref = std::get_if<BlockRef>(&operation.result)) {
        os << *ref << "\t";
    }
    for (const InputVariant& input : operation.input) {
        if (auto ref = std::get_if<ValueRef>(&input)) {
            os << *ref << "\t";
        } else if (auto ref = std::get_if<BlockRef>(&input)) {
            os << *ref << "\t";
        } else if (auto ref = std::get_if<ConstantValue>(&input)) {
            os << *ref << "\t";
        }
    }
    return os;
}

}// namespace NES::Interpreter