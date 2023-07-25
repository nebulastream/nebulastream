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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLELIMITOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLELIMITOPERATOR_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <QueryCompiler/CodeGenerator/LegacyExpression.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES::QueryCompilation::GeneratableOperators {

/**
 * @brief Generates a conditional limit operator, which limit the number of evicted tuples
 */
class GeneratableLimitOperator : public GeneratableOperator {
  public:
    /**
     * @brief Creates a new generatable limit operator, which evaluates a predicate on all input records.
     * @param inputSchema input schema of the incoming records.
     * @param limit
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr create(SchemaPtr inputSchema, uint64_t limit);

    /**
     * @brief Creates a new generatable limit operator, which evaluates a predicate on all input records.
    * @param id operator id
    * @param inputSchema input schema of the incoming records.
    * @param limit
    * @return GeneratableOperatorPtr
    */
    static GeneratableOperatorPtr create(OperatorId id, SchemaPtr inputSchema, uint64_t limit);

    /**
    * @brief Code generation function for the execute call of an operator.
    * The execute function is called for each tuple buffer consumed by this operator.
    * @param codegen reference to the code generator.
    * @param context reference to the current pipeline context.
    */
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;

    /**
    * @brief get the limit.
    * @return uint64_t
    */
    uint64_t getLimit();

    ~GeneratableLimitOperator() noexcept override = default;

  private:
    GeneratableLimitOperator(OperatorId id, const SchemaPtr& inputSchema, uint64_t limit);
    uint64_t limit;
};

}// namespace NES::QueryCompilation::GeneratableOperators
#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLELIMITOPERATOR_HPP_
