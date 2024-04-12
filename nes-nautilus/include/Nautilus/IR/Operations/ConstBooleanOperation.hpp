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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_CONSTBOOLEANOPERATION_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_CONSTBOOLEANOPERATION_HPP_

#include <Nautilus/IR/Operations/Operation.hpp>
#include <string>

namespace NES::Nautilus::IR::Operations {

class ConstBooleanOperation final : public Operation {
  public:
    explicit ConstBooleanOperation(OperationIdentifier identifier, bool value);
    ~ConstBooleanOperation() override = default;
    bool getValue() const;
    [[nodiscard]] std::string toString() const override;
    static bool classof(const Operation* Op);

  private:
    bool constantValue;// Can also hold uInts
};

}// namespace NES::Nautilus::IR::Operations
#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_CONSTBOOLEANOPERATION_HPP_
