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

#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GeneratableWindowOperator.hpp>
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Runtime/RuntimeHeaders.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableWindowOperator::GeneratableWindowOperator(OperatorId id,
                                                     SchemaPtr inputSchema,
                                                     SchemaPtr outputSchema,
                                                     Windowing::WindowOperatorHandlerPtr operatorHandler)
    : OperatorNode(id), GeneratableOperator(id, std::move(inputSchema), std::move(outputSchema)),
      operatorHandler(std::move(operatorHandler)) {}

void GeneratableWindowOperator::generateHeaders(PipelineContextPtr context){
    context->addHeaders({ WINDOW_OPERATOR_HANDLER,
                          AGGREGATION_WINDOW_HANDLER,
                          EXECUTABLE_COMPLETE_AGGR_TRIGGER_ACTION,
                          LOGICAL_WINDOW_DEFINITION,
                          WINDOW_MANAGER,
                          //transitive headers:
                          STATE_VARIABLE,
                          WINDOW_SLICE_STORE,
                           });

}


}// namespace NES::QueryCompilation::GeneratableOperators