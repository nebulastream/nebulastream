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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_JOINING_GENERATABLEBATCHJOINBUILDOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_JOINING_GENERATABLEBATCHJOINBUILDOPERATOR_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableBatchJoinOperator.hpp>

namespace NES::QueryCompilation::GeneratableOperators {

/**
 * @brief Defines the code generation for the build side of a batch join operator.
 */
class GeneratableBatchJoinBuildOperator : public GeneratableBatchJoinOperator {
  public:
    /**
     * @brief Creates a new generatable join build operator.
     * @param inputSchema the input schema for the operator.
     * @param outputSchema the output schema for the operator.
     * @param batchJoinOperatorHandler the join operator handler
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr create(SchemaPtr inputSchema,
                                         SchemaPtr outputSchema,
                                         Join::Experimental::BatchJoinOperatorHandlerPtr batchJoinOperatorHandler);

    /**
     * @brief Creates a new generatable join build operator.
     * @param id operator id
     * @param inputSchema the input schema for the operator.
     * @param outputSchema the output schema for the operator.
     * @param batchJoinOperatorHandler the join operator handler
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr create(OperatorId id,
                                         SchemaPtr inputSchema,
                                         SchemaPtr outputSchema,
                                         Join::Experimental::BatchJoinOperatorHandlerPtr batchJoinOperatorHandler);
    /**
    * @brief Code generation function for the execute call of an operator.
    * The execute function is called for each tuple buffer consumed by this operator.
    * @param codegen reference to the code generator.
    * @param context reference to the current pipeline context.
    */
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    /**
     * @brief Code generation function for the open call of an operator.
     * The open function is called once per operator to initialize local state by a single thread.
     * Sets up batch join handler.
     * @param codegen reference to the code generator.
     * @param context reference to the current pipeline context.
     */
    void generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;

  protected:
    GeneratableBatchJoinBuildOperator(OperatorId id,
                                      SchemaPtr inputSchema,
                                      SchemaPtr outputSchema,
                                      Join::Experimental::BatchJoinOperatorHandlerPtr batchJoinOperatorHandler);
};
}// namespace NES::QueryCompilation::GeneratableOperators
#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_JOINING_GENERATABLEBATCHJOINBUILDOPERATOR_HPP_
