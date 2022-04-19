#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_SSACREATIONPHASE_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_SSACREATIONPHASE_HPP_
#include <Interpreter/Tracer.hpp>
#include <set>

namespace NES::Interpreter {

class SSACreationPhase {
  public:
    ExecutionTrace apply(ExecutionTrace&& trace) {
        auto phaseContext = SSACreationPhaseContext(std::move(trace));
        return phaseContext.process();
    };

  private:
    class SSACreationPhaseContext {
      public:
        SSACreationPhaseContext(ExecutionTrace&& trace) : trace(std::move(trace)){};
        ExecutionTrace process() {
            auto& returnBlock = trace.getBlock(trace.returnRef->blockId);
            processBlock(returnBlock);
            removeAssignments();
            return std::move(trace);
        }

      private:
        bool isLocalValueRef(Block& block, ValueRef& type, uint32_t operationIndex) {
            for (uint32_t i = 0; i < operationIndex; i++) {
                auto& resOperation = block.operations[i];
                if (auto resultValueRef = std::get_if<ValueRef>(&resOperation.result)) {
                    if (resultValueRef->blockId == type.blockId && resultValueRef->operationId == type.operationId) {
                        return true;
                    }
                }
            }
            return std::find(block.arguments.begin(), block.arguments.end(), type) != block.arguments.end();
        }
        void processVariableRef(Block& block, ValueRef& type, uint32_t operationIndex) {
            if (isLocalValueRef(block, type, operationIndex)) {
                // variable is a local ref -> don't do anything.
            } else {
                // The valeRef references a different block, so we have to add it to the local arguments and append it to the pre-predecessor calls
                block.addArgument(type);
                // add to parameters in parent blocks
                for (auto& predecessor : block.predecessors) {
                    // add to final call
                    auto& predBlock = trace.getBlock(predecessor);
                    auto& lastOperation = predBlock.operations.back();
                    if (lastOperation.op == JMP || lastOperation.op == CMP) {
                        for (auto& input : lastOperation.input) {
                            auto& blockRef = std::get<BlockRef>(input);
                            if (blockRef.block == block.blockId) {
                                // TODO check if we contain the type already.
                                blockRef.arguments.emplace_back(type);
                            }
                        }
                    } else {
                        NES_THROW_RUNTIME_ERROR("Last operation of pred block should be JMP or CMP");
                    }
                }
            }
        }
        void processBlockRef(Block& block, BlockRef& blockRef, uint32_t operationIndex) {
            for (auto& input : blockRef.arguments) {
                processVariableRef(block, input, operationIndex);
            }
        };

        void removeAssignments() {
            for (Block& block : trace.getBlocks()) {
                std::unordered_map<ValueRef, ValueRef, ValueRefHasher> assignmentMap;
                for (uint64_t i = 0; i < block.operations.size(); i++) {
                    auto& operation = block.operations[i];
                    if (operation.op == ASSIGN) {
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
                    return item.op == ASSIGN;
                });
            }
        }

        void processBlock(Block& block) {
            for (int64_t i = block.operations.size() - 1; i >= 0; i--) {
                auto& operation = block.operations[i];
                // process input for each variable
                for (auto& input : operation.input) {
                    if (auto* valueRef = std::get_if<ValueRef>(&input)) {
                        processVariableRef(block, *valueRef, i);
                    } else if (auto* blockRef = std::get_if<BlockRef>(&input)) {
                        processBlockRef(block, *blockRef, i);
                    }
                }
            }
            processedBlocks.emplace(block.blockId);
            for (auto pred : block.predecessors) {
                auto& predBlock = trace.getBlock(pred);
                if (!processedBlocks.contains(pred))
                    processBlock(predBlock);
            }
        }

      private:
        ExecutionTrace trace;
        std::set<uint32_t> processedBlocks;
    };
};

}// namespace NES::Interpreter
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_SSACREATIONPHASE_HPP_
