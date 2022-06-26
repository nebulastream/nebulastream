//
// Created by pgrulich on 24.06.22.
//

#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_BASICBLOCKS_BASICBLOCKINVOCATION_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_BASICBLOCKS_BASICBLOCKINVOCATION_HPP_
#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
namespace NES::ExecutionEngine::Experimental::IR::Operations {

class BasicBlockInvocation {
  public:
    BasicBlockInvocation();
    void setBlock(BasicBlockPtr block);
    BasicBlockPtr getBlock();
    void addArgument(OperationPtr argument);
    void removeArgument(uint64_t argumentIndex);
    std::vector<OperationPtr> getArguments();
  private:
    BasicBlockPtr basicBlock;
    std::vector<OperationWPtr> operations;
};

}// namespace NES::ExecutionEngine::Experimental::IR::Operations
#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_NESIR_BASICBLOCKS_BASICBLOCKINVOCATION_HPP_
