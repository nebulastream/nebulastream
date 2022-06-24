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

#ifndef NES_AND_OPERATION_HPP
#define NES_AND_OPERATION_HPP

#include <Experimental/NESIR/Operations/Operation.hpp>

namespace NES::ExecutionEngine::Experimental::IR::Operations {

class AndOperation : public Operation {
  public:
    AndOperation(std::string identifier, OperationPtr leftInput, OperationPtr rightInput);
    ~AndOperation() override = default;

    std::string getIdentifier();
    OperationPtr getLeftInput();
    OperationPtr getRightInput();

    std::string toString() override;
    bool classof(const Operation* Op);

  private:
    std::string identifier;
    OperationWPtr leftInput;
    OperationWPtr rightInput;
};
}// namespace NES::ExecutionEngine::Experimental::IR::Operations
#endif//NES_AND_OPERATION_HPP
