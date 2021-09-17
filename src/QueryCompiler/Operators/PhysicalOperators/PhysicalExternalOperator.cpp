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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalExternalOperator.hpp>
#include <utility>
namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalExternalOperator::PhysicalExternalOperator(OperatorId id,
                                                   SchemaPtr inputSchema,
                                                   SchemaPtr outputSchema,
                                                   Runtime::Execution::ExecutablePipelineStagePtr executablePipelineStage)
    : OperatorNode(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
      executablePipelineStage(std::move(executablePipelineStage)) {}

PhysicalOperatorPtr
PhysicalExternalOperator::create(const SchemaPtr& inputSchema,
                                 const SchemaPtr& outputSchema,
                                 const Runtime::Execution::ExecutablePipelineStagePtr& executablePipelineStage) {
    return create(Util::getNextOperatorId(), inputSchema, outputSchema, executablePipelineStage);
}
PhysicalOperatorPtr
PhysicalExternalOperator::create(OperatorId id,
                                 const SchemaPtr& inputSchema,
                                 const SchemaPtr& outputSchema,
                                 const Runtime::Execution::ExecutablePipelineStagePtr& executablePipelineStage) {
    return std::make_shared<PhysicalExternalOperator>(id, inputSchema, outputSchema, executablePipelineStage);
}

std::string PhysicalExternalOperator::toString() const { return "PhysicalExternalOperator"; }

OperatorNodePtr PhysicalExternalOperator::copy() { return create(id, inputSchema, outputSchema, executablePipelineStage); }

Runtime::Execution::ExecutablePipelineStagePtr PhysicalExternalOperator::getExecutablePipelineStage() {
    return executablePipelineStage;
}

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES