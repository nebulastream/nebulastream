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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_IRGRAPH_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_IRGRAPH_HPP_

#include <memory>
namespace NES::Nautilus::IR {

namespace Operations {
class FunctionOperation;
}

/**
 * @brief The IRGraph represents a fragment of nautilus ir.
 */
class IRGraph {
  public:
    IRGraph() = default;
    ~IRGraph() = default;
    std::shared_ptr<Operations::FunctionOperation> addRootOperation(std::shared_ptr<Operations::FunctionOperation> rootOperation);
    std::shared_ptr<Operations::FunctionOperation> getRootOperation();
    std::string toString();

  private:
    std::shared_ptr<Operations::FunctionOperation> rootOperation;
};

}// namespace NES::Nautilus::IR

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_IR_IRGRAPH_HPP_
