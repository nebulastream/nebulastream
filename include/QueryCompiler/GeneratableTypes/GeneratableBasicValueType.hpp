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

#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEBASICVALUETYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEBASICVALUETYPE_HPP_

#include <QueryCompiler/GeneratableTypes/GeneratableValueType.hpp>

namespace NES {

class BasicValue;
typedef std::shared_ptr<BasicValue> BasicValuePtr;

/**
 * @brief Generates code for basic values.
 */
class GeneratableBasicValueType : public GeneratableValueType {
  public:
    GeneratableBasicValueType(BasicValuePtr basicValue);

    /**
    * @brief Generate the code expression for this value type.
    * @return CodeExpressionPtr
    */
    CodeExpressionPtr getCodeExpression() override;

  private:
    BasicValuePtr value;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_GENERATABLEBASICVALUETYPE_HPP_
