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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_LOGICALOPERATIONS_BITWISEANDOPERATION_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_LOGICALOPERATIONS_BITWISEANDOPERATION_HPP_

#include <Nautilus/IR/Operations/Operation.hpp>

namespace NES::Nautilus::IR::Operations {

class BitWiseAndOperation : public Operation {
  public:
    /**
     * @brief Constructor for a BitWiseAndOperation
     * @param identifier
     * @param leftInput
     * @param rightInput
     */
    BitWiseAndOperation(OperationIdentifier identifier, OperationPtr leftInput, OperationPtr rightInput);

    /**
     * @brief Default deconstructor
     */
    ~BitWiseAndOperation() override = default;

    /**
     * @brief Retrieves the left input
     * @return OperationPtr
     */
    OperationPtr getLeftInput();

    /**
     * @brief Retrieves the left input
     * @return OperationPtr
     */
    OperationPtr getRightInput();

    /**
     * @brief Creates a string representation of this operation
     * @return std::string
     */
    std::string toString() override;

    /**
     * @brief Checks if the operation (Op) is of the same class
     * @param Op
     * @return boolean
     */
    bool classof(const Operation* Op);

  private:
    OperationWPtr leftInput;
    OperationWPtr rightInput;
};
}// namespace NES::Nautilus::IR::Operations
#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_LOGICALOPERATIONS_BITWISEANDOPERATION_HPP_
