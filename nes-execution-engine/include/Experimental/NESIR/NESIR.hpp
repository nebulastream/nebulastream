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

#ifndef NES_NESIR_HPP
#define NES_NESIR_HPP

#include "Experimental/NESIR/Operations/FunctionOperation.hpp"
#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <Experimental/NESIR/ExternalDataSource.hpp>

namespace NES::ExecutionEngine::Experimental::IR {
class NESIR {
  public:
    NESIR() = default;
    ~NESIR() = default;

    std::shared_ptr<Operations::FunctionOperation> addRootOperation(std::shared_ptr<Operations::FunctionOperation> rootOperation);
    std::shared_ptr<Operations::FunctionOperation> getRootOperation();
  private:
    std::shared_ptr<Operations::FunctionOperation> rootOperation;
};

}// namespace NESIR

#endif //NES_NESIR_HPP
