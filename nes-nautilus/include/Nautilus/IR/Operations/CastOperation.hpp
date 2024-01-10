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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_CASTOPERATION_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_CASTOPERATION_HPP_

#include <Nautilus/IR/Operations/Operation.hpp>

namespace NES::Nautilus::IR::Operations {

class CastOperation : public Operation {
  public:
    explicit CastOperation(OperationIdentifier identifier, OperationPtr input, Types::StampPtr targetStamp);
    ~CastOperation() override = default;

    std::string toString() override;

    OperationPtr getInput();
    void setInput(OperationWPtr newInput);

  private:
    OperationWPtr input;
};

}// namespace NES::Nautilus::IR::Operations
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_CASTOPERATION_HPP_
