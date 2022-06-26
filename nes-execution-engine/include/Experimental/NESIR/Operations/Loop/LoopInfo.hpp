#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_OPERATIONS_LOOP_LOOPINFO_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_OPERATIONS_LOOP_LOOPINFO_HPP_

#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <Experimental/NESIR/Operations/Operation.hpp>
#include <vector>
namespace NES::ExecutionEngine::Experimental::IR::Operations {

class LoopInfo {
  public:
    virtual bool isCountedLoop() { return false; }
    virtual ~LoopInfo() = default;
};

class DefaultLoopInfo : public LoopInfo {};

class CountedLoopInfo : public LoopInfo {
  public:
    OperationWPtr lowerBound;
    OperationWPtr upperBound;
    OperationWPtr stepSize;
    std::vector<OperationPtr> loopInitialIteratorArguments;
    std::vector<OperationPtr> loopBodyIteratorArguments;
    OperationPtr loopBodyInductionVariable;
    BasicBlockPtr loopBodyBlock;
    BasicBlockPtr loopEndBlock;
    bool isCountedLoop() override { return true; }
};

}// namespace NES::ExecutionEngine::Experimental::IR::Operations

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_OPERATIONS_LOOP_LOOPINFO_HPP_
