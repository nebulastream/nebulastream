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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_BUILTINVARIABLEOPERATION_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_BUILTINVARIABLEOPERATION_HPP_

#include <Nautilus/IR/Operations/Operation.hpp>

#include <Nautilus/Interface/DataTypes/BuiltIns/BuiltInVariable.hpp>

namespace NES::Nautilus::IR::Operations {

class BuiltInVariableOperation : public Operation {
public:
    BuiltInVariableOperation(OperationIdentifier identifier, std::shared_ptr<BuiltInVariable> builtInVariable);

    ~BuiltInVariableOperation() override = default;

    std::string getValue();

    std::string toString() override;

    static bool classof(const Operation* Op);

private:
    std::shared_ptr<BuiltInVariable> builtInVariable;
};

} // namespace NES::Nautilus::IR::Operations

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_BUILTINVARIABLEOPERATION_HPP_
