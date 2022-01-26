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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_JOINING_GENERATABLEJOINBUILDOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_JOINING_GENERATABLEJOINBUILDOPERATOR_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableJoinOperator.hpp>

namespace NES::QueryCompilation::GeneratableOperators {

/**
 * @brief Defines the code generation for the build side of a join operator.
 */
class GeneratableJoinBuildOperator : public GeneratableJoinOperator {
  public:
    /**
     * @brief Creates a new generatable join build operator.
     * @param inputSchema the input schema for the operator.
     * @param outputSchema the output schema for the operator.
     * @param operatorHandler the join operator handler
     * @param buildSide indicator if this is the left or right build side.
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr
    create(SchemaPtr inputSchema, SchemaPtr outputSchema, Join::JoinOperatorHandlerPtr operatorHandler, JoinBuildSide buildSide);

    /**
     * @brief Creates a new generatable join build operator.
     * @param id operator id
     * @param inputSchema the input schema for the operator.
     * @param outputSchema the output schema for the operator.
     * @param operatorHandler the join operator handler
     * @param buildSide indicator if this is the left or right build side.
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr create(OperatorId id,
                                         SchemaPtr inputSchema,
                                         SchemaPtr outputSchema,
                                         Join::JoinOperatorHandlerPtr operatorHandler,
                                         JoinBuildSide buildSide);

    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    void generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;

  protected:
    GeneratableJoinBuildOperator(OperatorId id,
                                 SchemaPtr inputSchema,
                                 SchemaPtr outputSchema,
                                 Join::JoinOperatorHandlerPtr operatorHandler,
                                 JoinBuildSide buildSide);
    JoinBuildSide buildSide;
};
}// namespace NES::QueryCompilation::GeneratableOperators
#endif  // NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_JOINING_GENERATABLEJOINBUILDOPERATOR_HPP_
