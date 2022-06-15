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
