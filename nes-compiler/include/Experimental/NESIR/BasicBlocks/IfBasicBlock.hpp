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

#ifndef NES_IFBASICBLOCK_HPP
#define NES_IFBASICBLOCK_HPP

#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>

namespace NES {
class IfBasicBlock : public BasicBlock {
  public:
    IfBasicBlock(const std::vector<OperationPtr>& operations, BasicBlockPtr nextIfBlock, BasicBlockPtr nextElseBlock);
    ~IfBasicBlock() override = default;

  private:
    BasicBlockPtr nextIfBlock;
    BasicBlockPtr nextElseBlock;
};
}// namespace NES
#endif//NES_IFBASICBLOCK_HPP
