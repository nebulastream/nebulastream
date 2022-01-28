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

#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableProjectionOperator.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableProjectionOperator::GeneratableProjectionOperator(OperatorId id,
                                                             SchemaPtr inputSchema,
                                                             SchemaPtr outputSchema,
                                                             std::vector<ExpressionNodePtr> expressions)
    : OperatorNode(id), GeneratableOperator(id, std::move(inputSchema), std::move(outputSchema)),
      expressions(std::move(expressions)) {}

GeneratableOperatorPtr GeneratableProjectionOperator::create(OperatorId id,
                                                             SchemaPtr inputSchema,
                                                             SchemaPtr outputSchema,
                                                             std::vector<ExpressionNodePtr> expressions) {
    return std::make_shared<GeneratableProjectionOperator>(
        GeneratableProjectionOperator(id, std::move(inputSchema), std::move(outputSchema), std::move(expressions)));
}

GeneratableOperatorPtr
GeneratableProjectionOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, std::vector<ExpressionNodePtr> expressions) {
    return create(Util::getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(expressions));
}

void GeneratableProjectionOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForProjection(expressions, context);
}

std::string GeneratableProjectionOperator::toString() const { return "GeneratableProjectionOperator"; }

OperatorNodePtr GeneratableProjectionOperator::copy() { return create(id, inputSchema, outputSchema, expressions); }

}// namespace NES::QueryCompilation::GeneratableOperators