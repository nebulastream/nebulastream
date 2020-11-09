/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEARRAYVALUETYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEARRAYVALUETYPE_HPP_

#include <Common/ValueTypes/ValueType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableValueType.hpp>
#include <string>
#include <vector>
namespace NES {

/**
 * @brief Generates code for array values.
 * To this end it takes into account if the value is a string
 * todo we may want to factor string handling out in the future.
 */
class GeneratableArrayValueType : public GeneratableValueType {
  public:
    /**
     * @brief Constructs a new GeneratableArrayValueType
     * @param valueType the value type
     * @param values the values of the value type
     * @param isString indicates if this array value represents a string.
     */
    GeneratableArrayValueType(ValueTypePtr valueType, std::vector<std::string> values, bool isString = false);

    /**
     * @brief Generates code expresion, which represents this value.
     * @return
     */
    CodeExpressionPtr getCodeExpression() override;

  private:
    ValueTypePtr valueType;
    std::vector<std::string> values;
    bool isString;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEBASICVALUETYPE_HPP_
