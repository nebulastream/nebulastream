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

#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator/TranslateToLegacyExpression.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableFilterOperatorPredicated.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableFilterOperatorPredicated::GeneratableFilterOperatorPredicated(OperatorId id, const SchemaPtr& inputSchema, ExpressionNodePtr predicate)
    : OperatorNode(id), GeneratableOperator(id, inputSchema, inputSchema), predicate(std::move(predicate)) {}

GeneratableOperatorPtr GeneratableFilterOperatorPredicated::create(SchemaPtr inputSchema, ExpressionNodePtr predicate) {
    return create(UtilityFunctions::getNextOperatorId(), std::move(inputSchema), std::move(predicate));
}
GeneratableOperatorPtr GeneratableFilterOperatorPredicated::create(OperatorId id, SchemaPtr inputSchema, ExpressionNodePtr predicate) {
    return std::make_shared<GeneratableFilterOperatorPredicated>(
        GeneratableFilterOperatorPredicated(id, std::move(inputSchema), std::move(predicate)));
}

void GeneratableFilterOperatorPredicated::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    // todo remove if code gen can handle expressions
    auto legacyPredicate = TranslateToLegacyExpression::create()->transformExpression(predicate);
    codegen->generateCodeForFilterPredicated(std::dynamic_pointer_cast<Predicate>(legacyPredicate), context);
}

std::string GeneratableFilterOperatorPredicated::toString() const { return "GeneratableFilterOperatorPredicated"; }

OperatorNodePtr GeneratableFilterOperatorPredicated::copy() { return create(id, inputSchema, predicate); }

}// namespace NES::QueryCompilation::GeneratableOperators