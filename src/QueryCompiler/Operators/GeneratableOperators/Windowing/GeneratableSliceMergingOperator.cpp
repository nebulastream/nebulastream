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

#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GeneratableSliceMergingOperator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {
GeneratableOperatorPtr GeneratableSliceMergingOperator::create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                                               Windowing::WindowOperatorHandlerPtr operatorHandler) {
    return std::make_shared<GeneratableSliceMergingOperator>(
        GeneratableSliceMergingOperator(id, inputSchema, outputSchema, operatorHandler));
}

GeneratableOperatorPtr GeneratableSliceMergingOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::WindowOperatorHandlerPtr operatorHandler) {
    return create(UtilityFunctions::getNextOperatorId(), inputSchema, outputSchema, operatorHandler);
}

GeneratableSliceMergingOperator::GeneratableSliceMergingOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                                                 Windowing::WindowOperatorHandlerPtr operatorHandler)
    : OperatorNode(id), GeneratableWindowOperator(id, inputSchema, outputSchema, operatorHandler) {}

void GeneratableSliceMergingOperator::generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    // todo remove operator id from all runtime classes.
    auto operatorID = -1;
    // auto registerID = codegen->generateWindowSetup(windowDefinition, outputSchema, context, operatorID);
}

void GeneratableSliceMergingOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    //codegen->generateCodeForCombiningWindow(windowDefinition, generatableWindowAggregation, context, windowOperatorIndex);
}

const std::string GeneratableSliceMergingOperator::toString() const { return "GeneratableSliceMergingOperator"; }

OperatorNodePtr GeneratableSliceMergingOperator::copy() {
    return create(id, inputSchema, outputSchema, operatorHandler);
}

}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES