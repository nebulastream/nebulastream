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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_BUILTINS_BUILTINVARIABLE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_BUILTINS_BUILTINVARIABLE_HPP_

#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Nautilus {

/**
* @brief Base class for any class that represents a symbolic compiler built-in variable for computations in operator code.
*/
class BuiltInVariable : public Any {
public:
    BuiltInVariable(const TypeIdentifier* identifier);

    /**
     * @return The unique compiler built-in variable identifier.
     */
    virtual const std::string getIdentifier() const = 0;

    /**
     * @return A constant Value for allowing symbolic computation.
     */
    virtual const Value<> getAsValue() const = 0;
};

} // namespace NES::Nautilus

#endif // NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_BUILTINS_BUILTINVARIABLE_HPP_
