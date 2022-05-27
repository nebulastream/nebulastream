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

#ifndef NES_IFOPERATION_HPP
#define NES_IFOPERATION_HPP

#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>

namespace NES::ExecutionEngine::Experimental::IR::Operations {
class IfOperation : public Operation {
  public:
    IfOperation(std::string boolArgName, std::vector<std::string> thenBlockArgs = {}, std::vector<std::string> elseBlockArgs = {});
    ~IfOperation() override = default;

    std::string getBoolArgName();

    BasicBlockPtr setThenBranchBlock(BasicBlockPtr thenBlock);
    BasicBlockPtr setElseBranchBlock(BasicBlockPtr elseBlock);

    BasicBlockPtr getThenBranchBlock();
    BasicBlockPtr getElseBranchBlock();
    std::vector<std::string> getThenBlockArgs();
    std::vector<std::string> getElseBlockArgs();

  private:
    std::string boolArgName;
    BasicBlockPtr thenBranchBlock;
    BasicBlockPtr elseBranchBlock;
    std::vector<std::string> thenBlockArgs;
    std::vector<std::string> elseBlockArgs;
};
}// namespace NES
#endif//NES_IFOPERATION_HPP
