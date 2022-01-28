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

#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLETYPES_GENERATABLEARRAYVALUETYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLETYPES_GENERATABLEARRAYVALUETYPE_HPP_

#include <QueryCompiler/GeneratableTypes/GeneratableValueType.hpp>
#include <string>
#include <utility>
#include <vector>
namespace NES {
namespace QueryCompilation {
/**
 * @brief Generates code for array values.
 * To this end it takes into account if the value is a string
 * todo we may want to factor string handling out in the future.
 */
class GeneratableArrayValueType final : public GeneratableValueType {
  public:
    /**
     * @brief Constructs a new GeneratableArrayValueType
     * @param valueType the value type
     * @param values the values of the value type
     */
    inline GeneratableArrayValueType(ValueTypePtr valueTypePtr, std::vector<std::string>&& values) noexcept
        : valueType(std::move(valueTypePtr)), values(std::move(values)) {}

    inline GeneratableArrayValueType(ValueTypePtr valueTypePtr, std::vector<std::string> values) noexcept
        : valueType(std::move(valueTypePtr)), values(std::move(values)) {}

    /**
     * @brief Generates code expresion, which represents this value.
     * @return
     */
    [[nodiscard]] CodeExpressionPtr getCodeExpression() const noexcept final;

  private:
    ValueTypePtr const valueType;
    std::vector<std::string> const values;
};
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_GENERATABLETYPES_GENERATABLEARRAYVALUETYPE_HPP_
