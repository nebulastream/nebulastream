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
#ifndef NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_CE_P_GENERATABLE_CEP_ITERATION_OPERATOR_HPP_
#define NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_CE_P_GENERATABLE_CEP_ITERATION_OPERATOR_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Generates a CEP Iteration operator, which approves a match given a number of minimal and maximal repetions
 */
class GeneratableCEPIterationOperator : public GeneratableOperator {
  public:
    /**
     * @brief Creates a new generatable CEP Iteration operator
     * @param inputSchema the input schema
     * @param outputSchema the output schema
     * @param minIteration, the minimal number of event occurrence
     * @param maxIteration, the maximal number of event occurrence
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr
    create(SchemaPtr inputSchema, SchemaPtr outputSchema, uint64_t minIteration, uint64_t maxIteration);

    /**
     * @brief Creates a new generatable CEP Iteration operator
     * @param id operator id
     * @param inputSchema the input schema
     * @param outputSchema the output schema
     * @param minIteration, the minimal number of event occurrence
     * @param maxIteration, the maximal number of event occurrence
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr
    create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, uint64_t minIteration, uint64_t maxIteration);

    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    std::string toString() const override;

    OperatorNodePtr copy() override;

  private:
    GeneratableCEPIterationOperator(OperatorId id,
                                    SchemaPtr inputSchema,
                                    SchemaPtr outputSchema,
                                    uint64_t minIteration,
                                    uint64_t maxIteration);
    const uint64_t minIteration;
    const uint64_t maxIteration;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif  // NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_CE_P_GENERATABLE_CEP_ITERATION_OPERATOR_HPP_
