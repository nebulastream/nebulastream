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
#ifndef NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_GENERATABLE_FILTER_OPERATOR_HPP_
#define NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_GENERATABLE_FILTER_OPERATOR_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <QueryCompiler/CodeGenerator/LegacyExpression.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Generates a conditional filter operator, which evaluates a predicate on all input records.
 */
class GeneratableFilterOperator : public GeneratableOperator {
  public:
    /**
     * @brief Creates a new generatable filer operator, which evaluates a predicate on all input records.
     * @param inputSchema input schema of the incoming records.
     * @param predicate the predicate we evaluate
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr create(SchemaPtr inputSchema, ExpressionNodePtr predicate);

    /**
    * @brief Creates a new generatable filer operator, which evaluates a predicate on all input records.
    * @param id operator id
    * @param inputSchema input schema of the incoming records.
    * @param predicate the predicate we evaluate
    * @return GeneratableOperatorPtr
    */
    static GeneratableOperatorPtr create(OperatorId id, SchemaPtr inputSchema, ExpressionNodePtr predicate);
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;
    /**
    * @brief get the filter predicate.
    * @return PredicatePtr
    */
    ExpressionNodePtr getPredicate();

    ~GeneratableFilterOperator() noexcept override = default;

  private:
    GeneratableFilterOperator(OperatorId id, const SchemaPtr& inputSchema, ExpressionNodePtr predicate);
    ExpressionNodePtr predicate;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif// NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_GENERATABLE_FILTER_OPERATOR_HPP_
