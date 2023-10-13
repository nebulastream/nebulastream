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

#include <Operators/Expressions/ExpressionNode.hpp>
#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator/TranslateToLegacyExpression.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableInferModelOperator.hpp>
#include <Util/Core.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

GeneratableInferModelOperator::GeneratableInferModelOperator(OperatorId id,
                                                             SchemaPtr inputSchema,
                                                             SchemaPtr outputSchema,
                                                             std::string model,
                                                             std::vector<ExpressionItemPtr> inputFields,
                                                             std::vector<ExpressionItemPtr> outputFields,
                                                             InferModel::InferModelOperatorHandlerPtr operatorHandler)
    : OperatorNode(id), GeneratableOperator(id, inputSchema, outputSchema), model(model), inputFields(inputFields),
      outputFields(outputFields), operatorHandler(operatorHandler) {}

GeneratableOperatorPtr GeneratableInferModelOperator::create(SchemaPtr inputSchema,
                                                             SchemaPtr outputSchema,
                                                             std::string model,
                                                             std::vector<ExpressionItemPtr> inputFields,
                                                             std::vector<ExpressionItemPtr> outputFields,
                                                             InferModel::InferModelOperatorHandlerPtr operatorHandler) {
    return create(getNextOperatorId(), inputSchema, outputSchema, model, inputFields, outputFields, operatorHandler);
}

GeneratableOperatorPtr GeneratableInferModelOperator::create(OperatorId id,
                                                             SchemaPtr inputSchema,
                                                             SchemaPtr outputSchema,
                                                             std::string model,
                                                             std::vector<ExpressionItemPtr> inputFields,
                                                             std::vector<ExpressionItemPtr> outputFields,
                                                             InferModel::InferModelOperatorHandlerPtr operatorHandler) {
    return std::make_shared<GeneratableInferModelOperator>(
        GeneratableInferModelOperator(id, inputSchema, outputSchema, model, inputFields, outputFields, operatorHandler));
}

void GeneratableInferModelOperator::generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateInferModelSetup(context, operatorHandler);
}

void GeneratableInferModelOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForInferModel(context, inputFields, outputFields);
}

std::string GeneratableInferModelOperator::toString() const { return "GeneratableInferModelOperator"; }

OperatorNodePtr GeneratableInferModelOperator::copy() {
    return create(id, inputSchema, outputSchema, model, inputFields, outputFields, operatorHandler);
}

}// namespace GeneratableOperators

}// namespace QueryCompilation
}// namespace NES