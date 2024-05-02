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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_BUILTINS_CUDA_FIELDACCESS_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_BUILTINS_CUDA_FIELDACCESS_HPP_

#include <Nautilus/Interface/DataTypes/BuiltIns/BuiltInVariable.hpp>

namespace NES::Nautilus {

class BlockDim;

/**
 * @brief This class represents the access of named fields of structured data such as vectors.
 */
class FieldAccess : public BuiltInVariable {
public:
    static const inline auto type = TypeIdentifier::create<FieldAccess>();

    FieldAccess(std::shared_ptr<BuiltInVariable> builtInVariable, std::string fieldName);

    const std::string getIdentifier() const override;

    IR::Types::StampPtr getType() const override;

    std::shared_ptr<Any> copy() override;

    const Value<> getAsValue() const override;

private:
    std::shared_ptr<BuiltInVariable> builtInVariable;
    std::string fieldName;
};

} // namespace NES::Nautilus

#endif // NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_BUILTINS_CUDA_FIELDACCESS_HPP_
