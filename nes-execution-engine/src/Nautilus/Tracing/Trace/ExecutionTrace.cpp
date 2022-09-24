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

#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/Trace/OperationRef.hpp>

namespace NES::Nautilus::Tracing {

ExecutionTrace::ExecutionTrace() : tagMap(), blocks() { createBlock(); };

void ExecutionTrace::addOperation(TraceOperation& operation) {
    if (blocks.empty()) {
        createBlock();
    }
    operation.operationRef = std::make_shared<OperationRef>(currentBlock, blocks[currentBlock].operations.size());
    blocks[currentBlock].operations.emplace_back(operation);
    if (operation.op == RETURN) {
        returnRef = operation.operationRef;
    }
}

void ExecutionTrace::addArgument(const ValueRef& argument) {
    if (std::find(arguments.begin(), arguments.end(), argument) == arguments.end()) {
        this->arguments.emplace_back(argument);
    }
}

uint32_t ExecutionTrace::createBlock() {

    // add first block
    if (blocks.empty()) {
        // add arguments to first block
        blocks.emplace_back(blocks.size());
        blocks[0].arguments = arguments;
        return blocks.size() - 1;
    }
    blocks.emplace_back(blocks.size());
    return blocks.size() - 1;
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

Block& ExecutionTrace::processControlFlowMerge(uint32_t blockIndex, uint32_t operationIndex) {
    // perform a control flow merge and merge the current block with operations in some other block.
    // create new block
    auto mergedBlockId = createBlock();
    auto& mergeBlock = getBlock(mergedBlockId);
    mergeBlock.type = Block::ControlFlowMerge;
    // move operation to new block
    auto& oldBlock = getBlock(blockIndex);
    // copy everything between opId and end;

    for (uint32_t opIndex = operationIndex; opIndex < oldBlock.operations.size(); opIndex++) {
        auto sourceOperation = oldBlock.operations[opIndex];
        if (sourceOperation.operationRef == nullptr) {
            sourceOperation.operationRef = std::make_shared<OperationRef>(0, 0);
        }
        // if(sourceOperation.operationRef != nullptr){

        sourceOperation.operationRef->blockId = mergedBlockId;
        sourceOperation.operationRef->operationId = mergeBlock.operations.size();
        //}
        mergeBlock.operations.emplace_back(sourceOperation);
    }

    auto oldBlockRef = BlockRef(mergedBlockId);

    // rename vars in new block
    // todo reduce complexity
    /*
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
*/
    // remove content beyond opID
    oldBlock.operations.erase(oldBlock.operations.begin() + operationIndex, oldBlock.operations.end());
    oldBlock.operations.emplace_back(
        TraceOperation(JMP, ValueRef(0, 0, ExecutionEngine::Experimental::IR::Types::StampFactory::createVoidStamp()), {oldBlockRef}));
    auto operation = TraceOperation(JMP, ValueRef(0, 0, ExecutionEngine::Experimental::IR::Types::StampFactory::createVoidStamp()), {BlockRef(mergedBlockId)});
    addOperation(operation);

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


std::ostream& operator<<(std::ostream& os, const ExecutionTrace& executionTrace) {
    for (size_t i = 0; i < executionTrace.blocks.size(); i++) {
        os << "Block" << i ;

        os << executionTrace.blocks[i];
    }
    return os;
}

std::vector<ValueRef> ExecutionTrace::getArguments() {
    return arguments;
}


}// namespace NES::Nautilus::Tracing