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

#ifndef NES_BASICBLOCK_HPP
#define NES_BASICBLOCK_HPP

#include <vector>
#include <memory>

#include <Experimental/NESIR/Operations/Operation.hpp>
namespace NES {

class BasicBlock {
  public:

    /**
     * @brief BasicBlock used for control flow in NES IR
     * @param Operations: A list of Operations that are executed in the BasicBlock.
     * @param nextBlock : The BasicBlock that is next in the control flow of the execution.
     */
    explicit BasicBlock(std::vector<OperationPtr> operations);
    virtual ~BasicBlock() = default;
    [[nodiscard]] std::vector<OperationPtr> getOperations();

  private:
    std::vector<OperationPtr> operations;
};
using BasicBlockPtr = std::unique_ptr<BasicBlock>;
} // namespace NES
#endif//NES_BASICBLOCK_HPP
