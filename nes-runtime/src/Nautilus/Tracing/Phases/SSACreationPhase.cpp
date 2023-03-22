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
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/Trace/OperationRef.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Tracing {

std::shared_ptr<ExecutionTrace> SSACreationPhase::apply(std::shared_ptr<ExecutionTrace> trace) {
    // Initialize a new context and perform the inference of the SSA values
    auto phaseContext = SSACreationPhaseContext(std::move(trace));
    return phaseContext.process();
};

SSACreationPhase::SSACreationPhaseContext::SSACreationPhaseContext(std::shared_ptr<ExecutionTrace> trace)
    : trace(std::move(trace)) {}

std::shared_ptr<ExecutionTrace> SSACreationPhase::SSACreationPhaseContext::process() {
    trace->getBlock(0).arguments = trace->getArguments();
    // In the first step we get the return block, which contains the return call.
    // Starting with this block we trace all inputs
    auto& returnBlock = trace->getBlock(trace->getReturn()->blockId);
    processBlock(returnBlock);
    // Eliminate all assign operations. We only needed them to create the SSA from.
    removeAssignOperations();
    // Finally we make all block arguments unique to their local block.
    // As a result two blocks, can't use the same value references.
    makeBlockArgumentsUnique();
    return std::move(trace);
}

bool SSACreationPhase::SSACreationPhaseContext::isLocalValueRef(Block& block, ValueRef& type, uint32_t operationIndex) {
    // A value ref is defined in the local scope, if it is the result of an operation before the operationIndex
    for (uint32_t i = 0; i < operationIndex; i++) {
        auto& resOperation = block.operations[i];
        if (auto resultValueRef = std::get_if<ValueRef>(&resOperation.result)) {
            if (resultValueRef->blockId == type.blockId && resultValueRef->operationId == type.operationId) {
                return true;
            }
        }
    }
    // check if the operation is defined in the block arguments
    return std::find(block.arguments.begin(), block.arguments.end(), type) != block.arguments.end();
}

void SSACreationPhase::SSACreationPhaseContext::processBlock(Block& block) {
    // Process the inputs of all operations in the current block
    for (int64_t i = block.operations.size() - 1; i >= 0; i--) {
        auto& operation = block.operations[i];
        // process input for each variable
        for (auto& input : operation.input) {
            if (auto* valueRef = std::get_if<ValueRef>(&input)) {
                processValueRef(block, *valueRef, i);
            } else if (auto* blockRef = std::get_if<BlockRef>(&input)) {
                processBlockRef(block, *blockRef, i);
            }
        }
    }
    processedBlocks.emplace(block.blockId);
    // Recursively process the predecessors of this block
    // If the current block is a control-flow merge it may have multiple predecessors.
    // We avoid visiting them again by checking the processedBlocks set.
    for (auto pred : block.predecessors) {
        auto& predBlock = trace->getBlock(pred);
        if (!processedBlocks.contains(pred)) {
            processBlock(predBlock);
        }
    }
}

void SSACreationPhase::SSACreationPhaseContext::processValueRef(Block& block, ValueRef& type, uint32_t operationIndex) {
    if (isLocalValueRef(block, type, operationIndex)) {
        // variable is a local ref -> don't do anything as the value is defined in the current block
    } else {
        // The valeRef references a different block, so we have to add it to the local arguments and append it to the pre-predecessor calls
        block.addArgument(type);
        // add to parameters in parent blocks
        for (auto& predecessor : block.predecessors) {
            // add to final call
            auto& predBlock = trace->getBlock(predecessor);
            auto& lastOperation = predBlock.operations.back();
            if (lastOperation.op == OpCode::JMP || lastOperation.op == OpCode::CMP) {
                for (auto& input : lastOperation.input) {
                    auto& blockRef = std::get<BlockRef>(input);
                    if (blockRef.block == block.blockId) {
                        // TODO check if we contain the type already.
                        blockRef.arguments.emplace_back(type);
                        // we changed the block an arguments, thus we have to revisit it.
                        if (processedBlocks.contains(predBlock.blockId)) {
                            processedBlocks.erase(predBlock.blockId);
                        }
                    }
                }
            } else {
                NES_THROW_RUNTIME_ERROR("Last operation of pred block should be JMP or CMP");
            }
        }
    }
}

void SSACreationPhase::SSACreationPhaseContext::processBlockRef(Block& block, BlockRef& blockRef, uint32_t operationIndex) {
    // a block ref has a set of arguments, which are handled the same as all other value references.
    for (auto& input : blockRef.arguments) {
        processValueRef(block, input, operationIndex);
    }
};

void SSACreationPhase::SSACreationPhaseContext::removeAssignOperations() {
    // Iterate over all block and eliminate the ASSIGN operation.
    for (Block& block : trace->getBlocks()) {
        std::unordered_map<ValueRef, ValueRef, ValueRefHasher> assignmentMap;
        for (uint64_t i = 0; i < block.operations.size(); i++) {
            auto& operation = block.operations[i];
            if (operation.op == OpCode::CMP) {
                if (auto* valueRef = std::get_if<ValueRef>(&operation.result)) {
                    auto foundAssignment = assignmentMap.find(*valueRef);
                    if (foundAssignment != assignmentMap.end()) {
                        valueRef->blockId = foundAssignment->second.blockId;
                        valueRef->operationId = foundAssignment->second.operationId;
                    }
                }
            }
            if (operation.op == OpCode::ASSIGN) {
                assignmentMap[get<ValueRef>(operation.result)] = get<ValueRef>(operation.input[0]);
            } else {
                for (auto& input : operation.input) {
                    if (auto* valueRef = std::get_if<ValueRef>(&input)) {
                        auto foundAssignment = assignmentMap.find(*valueRef);
                        if (foundAssignment != assignmentMap.end()) {
                            valueRef->blockId = foundAssignment->second.blockId;
                            valueRef->operationId = foundAssignment->second.operationId;
                        }
                    } else if (auto* blockRef = std::get_if<BlockRef>(&input)) {
                        for (auto& blockArgument : blockRef->arguments) {
                            auto foundAssignment = assignmentMap.find(blockArgument);
                            if (foundAssignment != assignmentMap.end()) {
                                blockArgument.blockId = foundAssignment->second.blockId;
                                blockArgument.operationId = foundAssignment->second.operationId;
                            }
                        }
                    }
                }
            }
        }

        std::erase_if(block.operations, [&](const auto& item) {
            return item.op == OpCode::ASSIGN;
        });
    }
}
void SSACreationPhase::SSACreationPhaseContext::makeBlockArgumentsUnique() {
    for (Block& block : trace->getBlocks()) {
        std::unordered_map<ValueRef, ValueRef, ValueRefHasher> blockArgumentMap;

        // iterate over all arguments of this block and create new ValRefs if the argument ref is not local.
        for (uint64_t argIndex = 0; argIndex < block.arguments.size(); argIndex++) {
            auto argRef = block.arguments[argIndex];
            if (argRef.blockId != block.blockId) {
                auto newLocalRef = ValueRef(block.blockId, block.operations.size() + blockArgumentMap.size() + 100, argRef.type);
                blockArgumentMap[argRef] = newLocalRef;
                block.arguments[argIndex] = newLocalRef;
            }
        }

        // set the new ValRefs to all depending operations.
        for (uint64_t i = 0; i < block.operations.size(); i++) {
            auto& operation = block.operations[i];
            for (auto& input : operation.input) {
                if (auto* valueRef = std::get_if<ValueRef>(&input)) {
                    auto foundAssignment = blockArgumentMap.find(*valueRef);
                    if (foundAssignment != blockArgumentMap.end()) {
                        valueRef->blockId = foundAssignment->second.blockId;
                        valueRef->operationId = foundAssignment->second.operationId;
                    }
                } else if (auto* blockRef = std::get_if<BlockRef>(&input)) {
                    for (auto& blockArgument : blockRef->arguments) {
                        auto foundAssignment = blockArgumentMap.find(blockArgument);
                        if (foundAssignment != blockArgumentMap.end()) {
                            blockArgument.blockId = foundAssignment->second.blockId;
                            blockArgument.operationId = foundAssignment->second.operationId;
                        }
                    }
                }
            }
        }

        std::erase_if(block.operations, [&](const auto& item) {
            return item.op == OpCode::ASSIGN;
        });
    }
}

}// namespace NES::Nautilus::Tracing