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
#ifndef NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_WINDOWING_GENERATABLE_WINDOW_OPERATOR_HPP_
#define NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_WINDOWING_GENERATABLE_WINDOW_OPERATOR_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Base class for all generatable window operators.
 */
class GeneratableWindowOperator : public GeneratableOperator {
  protected:
    GeneratableWindowOperator(OperatorId id,
                              SchemaPtr inputSchema,
                              SchemaPtr outputSchema,
                              Windowing::WindowOperatorHandlerPtr operatorHandler);

    virtual ~GeneratableWindowOperator() noexcept = default;

    /**
     *
     * @return headers required by GeneratableWindowOperator
     */
    void generateHeaders(PipelineContextPtr context) override;

    Windowing::WindowOperatorHandlerPtr operatorHandler;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_WINDOWING_GENERATABLE_WINDOW_OPERATOR_HPP_
