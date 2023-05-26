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
#include <algorithm>
#include <sstream>

namespace NES::Nautilus::Tracing {

ExecutionTrace::ExecutionTrace() : blocks() { createBlock(); };

void ExecutionTrace::addOperation(TraceOperation& operation) {
    if (blocks.empty()) {
        createBlock();
    }
    operation.operationRef = std::make_shared<OperationRef>(currentBlock, blocks[currentBlock].operations.size());
    blocks[currentBlock].operations.emplace_back(operation);
    if (operation.op == OpCode::RETURN) {
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

Block& ExecutionTrace::processControlFlowMerge(uint32_t blockIndex, uint32_t operationIndex) {
    // perform a control flow merge and merge the current block with operations in some other block.
    // create new block
    auto mergedBlockId = createBlock();
    auto& mergeBlock = getBlock(mergedBlockId);
    mergeBlock.type = Block::Type::ControlFlowMerge;
    // move operation to new block
    auto& oldBlock = getBlock(blockIndex);
    // copy everything between opId and end;
    for (uint32_t opIndex = operationIndex; opIndex < oldBlock.operations.size(); opIndex++) {
        auto sourceOperation = oldBlock.operations[opIndex];
        if (sourceOperation.operationRef == nullptr) {
            sourceOperation.operationRef = std::make_shared<OperationRef>(0, 0);
        }
        sourceOperation.operationRef->blockId = mergedBlockId;
        sourceOperation.operationRef->operationId = mergeBlock.operations.size();
        mergeBlock.operations.emplace_back(sourceOperation);
    }

    auto oldBlockRef = BlockRef(mergedBlockId);

    // remove content beyond opID
    oldBlock.operations.erase(oldBlock.operations.begin() + operationIndex, oldBlock.operations.end());
    oldBlock.operations.emplace_back(
        TraceOperation(OpCode::JMP, ValueRef(0, 0, NES::Nautilus::IR::Types::StampFactory::createVoidStamp()), {oldBlockRef}));
    auto operation = TraceOperation(OpCode::JMP,
                                    ValueRef(0, 0, NES::Nautilus::IR::Types::StampFactory::createVoidStamp()),
                                    {BlockRef(mergedBlockId)});
    addOperation(operation);

    mergeBlock.predecessors.emplace_back(blockIndex);
    mergeBlock.predecessors.emplace_back(currentBlock);
    setCurrentBlock(mergedBlockId);

    //
    auto& lastMergeOperation = mergeBlock.operations[mergeBlock.operations.size() - 1];
    if (lastMergeOperation.op == OpCode::CMP || lastMergeOperation.op == OpCode::JMP) {
        for (auto& input : lastMergeOperation.input) {
            auto& blockRef = std::get<BlockRef>(input);
            auto& blockPredecessor = getBlock(blockRef.block).predecessors;
            std::replace(blockPredecessor.begin(), blockPredecessor.end(), blockIndex, mergedBlockId);
            std::replace(blockPredecessor.begin(), blockPredecessor.end(), currentBlock, mergedBlockId);
        }
    }

    return mergeBlock;
}

std::string ExecutionTrace::toString() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

std::ostream& operator<<(std::ostream& os, const ExecutionTrace& executionTrace) {
    for (size_t i = 0; i < executionTrace.blocks.size(); i++) {
        os << "Block" << i;

        os << executionTrace.blocks[i];
    }
    return os;
}

const std::vector<ValueRef>& ExecutionTrace::getArguments() { return arguments; }

std::shared_ptr<OperationRef> ExecutionTrace::getReturn() { return returnRef; }

}// namespace NES::Nautilus::Tracing