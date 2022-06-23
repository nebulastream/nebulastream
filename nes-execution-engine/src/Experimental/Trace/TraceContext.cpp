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

#include <Experimental/Interpreter/DataValue/Boolean.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/Util/Casting.hpp>
#include <Experimental/Trace/ExecutionTrace.hpp>
#include <Experimental/Trace/OperationRef.hpp>
#include <Experimental/Trace/TraceContext.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <execinfo.h>
#include <iostream>
#include <map>
namespace NES::ExecutionEngine::Experimental::Trace {

static thread_local TraceContext threadLocalTraceContext;
void initThreadLocalTraceContext() { threadLocalTraceContext = TraceContext(); }
TraceContext* getThreadLocalTraceContext() { return &threadLocalTraceContext; }

TraceContext::TraceContext() : executionTrace(std::make_unique<ExecutionTrace>()) {
    reset();
    startAddress = Tag::createCurrentAddress();
    std::cout << startAddress << std::endl;
}

void TraceContext::reset() {

    executionTrace->setCurrentBloc(0);
    currentOperationCounter = 0;
    executionTrace->tagMap.merge(executionTrace->localTagMap);
    executionTrace->localTagMap.clear();
};

ValueRef TraceContext::createNextRef(IR::Operations::Operation::BasicType type) { return ValueRef(executionTrace->getCurrentBlockIndex(), currentOperationCounter, type); }

std::shared_ptr<ExecutionTrace> TraceContext::getExecutionTrace() { return executionTrace; }

uint64_t TraceContext::createStartAddress() {
    void* buffer[20];
    // First add the RIP pointers
    int size = backtrace(buffer, 20);
   /* char** backtrace_functions = backtrace_symbols(buffer, size);
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
    }*/
    return (uint64_t) buffer[5];
}

/*
void TraceContext::trace(OpCode op, const Value& leftInput, const Value& rightInput, Value& result) {
    if (executionTrace->getCurrentBlock().operations.size() > currentOperationCounter) {
        // we should contain this operation
        auto operation = executionTrace->getCurrentBlock().operations[currentOperationCounter];
        if (operation.op == op) {
            std::cout << "Trace: handle same instruction " << std::endl;
            std::cout << executionTrace << std::endl;
            if (auto* valueRef = get_if<ValueRef>(&operation.input[0])) {
                executionTrace->checkInputReference(executionTrace->getCurrentBlockIndex(), *valueRef, leftInput.ref);
            }
            std::cout << executionTrace << std::endl;
            if (auto* valueRef = get_if<ValueRef>(&operation.input[1])) {
                executionTrace->checkInputReference(executionTrace->getCurrentBlockIndex(), *valueRef, rightInput.ref);
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
    if (executionTrace->tagMap.contains(tag)) {
        std::cout << "Control-flow merge" << result.value << std::endl;
        auto res = executionTrace->tagMap.find(tag);
        auto [blockId, opId] = res->second;
        // create new block
        auto& mergeBlock = executionTrace->processControlFlowMerge(blockId, opId);
        currentOperationCounter = mergeBlock.operations.size() - 1;
    } else if (executionTrace->localTagMap.contains(tag)) {
        std::cout << "Local Control-flow merge-> Loop" << result.value << std::endl;
        std::cout << "Control-flow merge" << result.value << std::endl;
        std::cout << executionTrace << std::endl;
        auto res = executionTrace->localTagMap.find(tag);
        auto [blockId, opId] = res->second;
        auto& mergeBlock = executionTrace->processControlFlowMerge(blockId, opId);
        auto mergeOperation = mergeBlock.operations.front();
        result.ref = std::get<ValueRef>(mergeOperation.result);
        std::cout << executionTrace << std::endl;
        currentOperationCounter = 1;

    } else {
        auto leftInputRef = executionTrace->findReference(leftInput.ref, leftInput.srcRef);
        auto rightInputRef = executionTrace->findReference(rightInput.ref, rightInput.srcRef);
        auto operation = Operation(op, result.ref, {leftInputRef, rightInputRef});
        executionTrace->addOperation(operation);
        executionTrace->localTagMap.emplace(
            std::make_pair(tag, std::make_tuple(executionTrace->getCurrentBlockIndex(), currentOperationCounter)));
        currentOperationCounter++;
    }
}
 */

Operation& TraceContext::getLastOperation() {
    auto& currentBlock = executionTrace->getCurrentBlock();
    //if (currentBlock.operations.size() <= currentOperationCounter) {
    //    return NULL;
    // }
    return currentBlock.operations[currentOperationCounter];
}

bool TraceContext::isExpectedOperation(OpCode opCode) {
    auto& currentBlock = executionTrace->getCurrentBlock();
    if (currentBlock.operations.size() <= currentOperationCounter) {
        return false;
    }
    auto currentOperation = currentBlock.operations[currentOperationCounter];
    // the next operation is a jump we transfer to that block.
    while (currentOperation.op == JMP) {
        executionTrace->setCurrentBloc(std::get<BlockRef>(currentOperation.input[0]).block);
        currentOperationCounter = 0;
        currentOperation = executionTrace->getCurrentBlock().operations[currentOperationCounter];
    }
    return currentOperation.op == opCode;
}

std::shared_ptr<OperationRef> TraceContext::isKnownOperation(Tag& tag) {
    if (executionTrace->tagMap.contains(tag)) {
        return executionTrace->tagMap.find(tag)->second;
    } else if (executionTrace->localTagMap.contains(tag)) {
        return executionTrace->localTagMap.find(tag)->second;
    }
    return nullptr;
}

void TraceContext::traceCMP(const ValueRef& valueRef, bool result) {
    uint32_t trueBlock;
    uint32_t falseBlock;
    if (!isExpectedOperation(CMP)) {
        trueBlock = executionTrace->createBlock();
        falseBlock = executionTrace->createBlock();
        executionTrace->getBlock(trueBlock).predecessors.emplace_back(executionTrace->getCurrentBlockIndex());
        executionTrace->getBlock(falseBlock).predecessors.emplace_back(executionTrace->getCurrentBlockIndex());
        auto operation = Operation(CMP, valueRef, {BlockRef(trueBlock), BlockRef(falseBlock)});
        executionTrace->addOperation(operation);
    } else {
        // we repeat the operation
        auto operation = executionTrace->getCurrentBlock().operations[currentOperationCounter];
        trueBlock = std::get<BlockRef>(operation.input[0]).block;
        falseBlock = std::get<BlockRef>(operation.input[1]).block;
    }

    // set next block
    if (result) {
        executionTrace->setCurrentBloc(trueBlock);
    } else {
        executionTrace->setCurrentBloc(falseBlock);
    }
    currentOperationCounter = 0;
}

void TraceContext::trace(Operation& operation) {
    // check if we repeat a known trace or if this is a new operation.
    // we are in a know operation if the operation at the current block[currentOperationCounter] is equal to the received operation.
    // std::cout << "Add operation: " << operation << std::endl;
    // std::cout << *executionTrace.get() << std::endl;
    if (!isExpectedOperation(operation.op)) {
        auto tag = Tag::createTag(startAddress);
        if (auto ref = isKnownOperation(tag)) {
            if (ref->blockId != this->executionTrace->getCurrentBlockIndex()) {
                // std::cout << "----------- CONTROL_FLOW_MERGE ------------" << std::endl;
                // std::cout << "----------- LAST OPERATION << " << operation << " ref (" << ref->blockId << "-" << ref->operationId
                //           << ")-----------" << std::endl;
                std::cout << *executionTrace.get() << std::endl;
                auto& mergeBlock = executionTrace->processControlFlowMerge(ref->blockId, ref->operationId);
                auto mergeOperation = mergeBlock.operations.front();
                currentOperationCounter = 1;
                return;
            } else {
                std::cout << "----------- Ignore CONTROL_FLOW_MERGE as it is in the same block------------" << std::endl;
            }
        }
        executionTrace->addOperation(operation);
        executionTrace->localTagMap.emplace(std::make_pair(tag, operation.operationRef));
    }

    currentOperationCounter++;
}


/*
void TraceContext::trace(OpCode op, const Value& input, Value& result) {

    if (executionTrace->getCurrentBlock().operations.size() > currentOperationCounter) {
        // we should contain this operation
        auto operation = executionTrace->getCurrentBlock().operations[currentOperationCounter];
        // pull next operation;
        if (operation.op == JMP) {
            executionTrace->setCurrentBloc(std::get<BlockRef>(operation.input[0]).block);
            currentOperationCounter = 0;
            operation = executionTrace->getCurrentBlock().operations[currentOperationCounter];
        }
        if (operation.op == op) {
            std::cout << "Trace: handle same instruction " << std::endl;
            if (op == CMP) {
                if (cast<Boolean>(input.value)->value) {
                    executionTrace->setCurrentBloc(std::get<BlockRef>(operation.input[0]).block);
                    currentOperationCounter = 0;
                    return;
                } else {
                    executionTrace->setCurrentBloc(std::get<BlockRef>(operation.input[1]).block);
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
    if (executionTrace->localTagMap.contains(tag)) {
        std::cout << "Local Control-flow merge-> Loop" << result.value << std::endl;
        std::cout << "Control-flow merge" << result.value << std::endl;
        std::cout << executionTrace << std::endl;
        auto res = executionTrace->localTagMap.find(tag);
        auto [blockId, opId] = res->second;
        auto& mergeBlock = executionTrace->processControlFlowMerge(blockId, opId);
        auto mergeOperation = mergeBlock.operations.front();
        result.ref = std::get<ValueRef>(mergeOperation.result);
        std::cout << executionTrace << std::endl;
        currentOperationCounter = 1;

    } else if (executionTrace->tagMap.contains(tag) && op != CMP) {
        std::cout << "Control-flow merge" << result.value << std::endl;
        auto res = executionTrace->tagMap.find(tag);
        auto [blockId, opId] = res->second;
        auto& mergeBlock = executionTrace->processControlFlowMerge(blockId, opId);
        auto mergeOperation = mergeBlock.operations.front();
        result.ref = std::get<ValueRef>(mergeOperation.result);
        std::cout << "Control-flow merge" << result.value << std::endl;
        currentOperationCounter = 1;
    } else if (op == CMP) {
        std::cout << "Trace: handle control-flow split. Current case: " << result.value << std::endl;

        auto trueBlock = executionTrace->createBlock();
        auto falseBlock = executionTrace->createBlock();
        executionTrace->getBlock(trueBlock).predecessors.emplace_back(executionTrace->getCurrentBlockIndex());
        executionTrace->getBlock(falseBlock).predecessors.emplace_back(executionTrace->getCurrentBlockIndex());
        auto operation = Operation(op, result.ref, {BlockRef(trueBlock), BlockRef(falseBlock)});
        executionTrace->addOperation(operation);
        executionTrace->tagMap.emplace(
            std::make_pair(tag, std::make_tuple(executionTrace->getCurrentBlockIndex(), currentOperationCounter)));
        if (cast<Boolean>(input.value)->value) {
            executionTrace->setCurrentBloc(trueBlock);
            currentOperationCounter = 0;
        } else {
            executionTrace->setCurrentBloc(falseBlock);
            currentOperationCounter = 0;
        }
    } else {
        if (op == OpCode::CONST) {
            auto constValue = input.value->copy();
            auto operation = Operation(op, result.ref, {ConstantValue(std::move(constValue))});
            executionTrace->addOperation(operation);
            executionTrace->localTagMap.emplace(
                std::make_pair(tag, std::make_tuple(executionTrace->getCurrentBlockIndex(), currentOperationCounter)));
            currentOperationCounter++;
        } else {
            auto operation = Operation(op, result.ref, {input.ref});
            executionTrace->addOperation(operation);
            executionTrace->localTagMap.emplace(
                std::make_pair(tag, std::make_tuple(executionTrace->getCurrentBlockIndex(), currentOperationCounter)));
            currentOperationCounter++;
        }
    }
}
 */


std::ostream& operator<<(std::ostream& os, const ValueRef& valueRef) {
    os << "$" << valueRef.blockId << "_" << valueRef.operationId;
    return os;
}
bool ValueRef::operator==(const ValueRef& rhs) const { return blockId == rhs.blockId && operationId == rhs.operationId; }
bool ValueRef::operator!=(const ValueRef& rhs) const { return !(rhs == *this); }

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
        } else if (auto ref = std::get_if<FunctionCallTarget>(&input)) {
            os << ref->functionName << "\t";
        }
    }
    return os;
}

}// namespace NES::ExecutionEngine::Experimental::Trace