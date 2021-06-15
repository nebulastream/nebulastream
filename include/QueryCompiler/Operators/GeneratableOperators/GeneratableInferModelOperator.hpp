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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEINFER_MODEL_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEINFER_MODEL_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Generates a conditional inferModel operator, which infers an ML model
 */
class GeneratableInferModelOperator : public GeneratableOperator {
  public:
    static GeneratableOperatorPtr create(SchemaPtr inputSchema, SchemaPtr outputSchema, std::string model, std::vector<ExpressionItemPtr> inputFields, std::vector<ExpressionItemPtr> outputFields);
    static GeneratableOperatorPtr create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, std::string model, std::vector<ExpressionItemPtr> inputFields, std::vector<ExpressionItemPtr> outputFields);
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    GeneratableInferModelOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, std::string model, std::vector<ExpressionItemPtr> inputFields, std::vector<ExpressionItemPtr> outputFields);
    const std::string model;
    const std::vector<ExpressionItemPtr> inputFields;
    const std::vector<ExpressionItemPtr> outputFields;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEINFER_MODEL_HPP_
