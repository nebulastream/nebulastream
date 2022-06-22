#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_PHASE_LOOPINFERENCEPHASE_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_PHASE_LOOPINFERENCEPHASE_HPP_

#include "Experimental/NESIR/Operations/AddIntOperation.hpp"
#include "Experimental/NESIR/Operations/IfOperation.hpp"
#include <Experimental/NESIR/NESIR.hpp>
#include <Experimental/NESIR/Operations/CompareOperation.hpp>
#include <Experimental/NESIR/Operations/ConstantIntOperation.hpp>
#include <Experimental/NESIR/Operations/LoopOperation.hpp>
namespace NES::ExecutionEngine::Experimental::IR {

class LoopInferencePhase {
  public:
    std::shared_ptr<NESIR> apply(std::shared_ptr<NESIR> ir) {
        auto loop = LoopInferencePhaseContext(ir);
        loop.process();
        return ir;
    }

  private:
    class LoopInferencePhaseContext {
      public:
        LoopInferencePhaseContext(std::shared_ptr<NESIR> ir) : ir(ir) {}
        void process() {
            auto rootBlock = ir->getRootOperation()->getFunctionBasicBlock();
            processBlock(rootBlock);
        }
        void processBlock(BasicBlockPtr block) {
            if (block->getTerminatorOp()->getOperationType() == Operations::Operation::LoopOp) {
                auto loop = std::static_pointer_cast<Operations::LoopOperation>(block->getTerminatorOp());
                processLoop(loop);
            }
        }

        void processLoop(std::shared_ptr<Operations::LoopOperation> loopOp) {
            auto loopHead = loopOp->getLoopHeadBlock();
            // TODO currently we assume that a head block ends with a if operation
            auto loopIfOp = std::static_pointer_cast<Operations::IfOperation>(loopOp->getLoopHeadBlock()->getOperations().back());
            // TODO find loop body
            auto loopBodyBlock = loopIfOp->getThenBranchBlock();
            inferCountedLoop(loopOp);
        }

        void inferCountedLoop(std::shared_ptr<Operations::LoopOperation> loopOp) {
            auto loopHeadOperations = loopOp->getLoopHeadBlock()->getOperations();
            auto compareValue = std::static_pointer_cast<Operations::ConstantIntOperation>(loopHeadOperations[0]);
            auto compareOperation = std::static_pointer_cast<Operations::CompareOperation>(loopHeadOperations[1]);
            auto ifOperation = std::static_pointer_cast<Operations::IfOperation>(loopHeadOperations[2]);
            auto loopBodyBlock = ifOperation->getThenBranchBlock();
            auto inductionVariable = compareOperation->getLeftArgName();
            uint32_t inductionVariableIndex = 0;
            for (auto argument : ifOperation->getThenBlockArgs()) {
                if (inductionVariable == argument) {
                    break;
                }
                inductionVariableIndex++;
            }

            auto inductionVariableNameLoopBody = loopBodyBlock->getInputArgs()[inductionVariableIndex];
            NES_DEBUG(inductionVariableNameLoopBody);
            // find add operation that depends on induction variable
            for (auto operation : loopBodyBlock->getOperations()) {
                if (operation->getOperationType() == Operations::Operation::AddOp) {
                    auto addOperation = std::static_pointer_cast<Operations::AddIntOperation>(operation);
                    if (addOperation->getLeftArgName() == inductionVariableNameLoopBody) {

                    } else if (addOperation->getRightArgName() == inductionVariableNameLoopBody) {

                    }
                }
            }
        }

      private:
        std::shared_ptr<NESIR> ir;
    };
};

}// namespace NES::ExecutionEngine::Experimental::IR
#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_PHASE_LOOPINFERENCEPHASE_HPP_
