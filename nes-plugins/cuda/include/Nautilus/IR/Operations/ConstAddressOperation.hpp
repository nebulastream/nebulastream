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

#ifndef NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_IR_OPERATIONS_CONSTADDRESSOPERATION_HPP_
#define NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_IR_OPERATIONS_CONSTADDRESSOPERATION_HPP_

#include <Nautilus/IR/Operations/Operation.hpp>

namespace NES::Nautilus::IR::Operations {

/**
 * @brief An Operation class that specifically hold a constant address.
 */
class ConstAddressOperation : public Operation {
  public:
    /**
     * @brief Creates a const address operation.
     * @param identifier identifier of the operation.
     * @param value the consts address to be encapsulated.
     */
    explicit ConstAddressOperation(OperationIdentifier identifier, void* value);

    ~ConstAddressOperation() override = default;

    /**
     * @brief Get the encapsulated const address.
     * @return The const address.
     */
    void* getValue();

    /**
     * @brief Gets a string representation of this const address operation.
     * @return A string representation of const address operation.
     */
    std::string toString() override;

    /**
     * @brief Checks if an operator is of type ConstAddressOperation.
     * @param Op operator to check.
     * @return true if Op is an instance of ConstAddressOperation, false otherwise.
     */
    static bool classof(const Operation* Op);

  private:
    void* constantValue;
};

}// namespace NES::Nautilus::IR::Operations

#endif// NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_IR_OPERATIONS_CONSTADDRESSOPERATION_HPP_
