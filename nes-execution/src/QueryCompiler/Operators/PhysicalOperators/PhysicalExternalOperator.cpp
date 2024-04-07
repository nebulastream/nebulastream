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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalExternalOperator.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalExternalOperator::PhysicalExternalOperator(OperatorId id,
                                                   StatisticId statisticId,
                                                   SchemaPtr inputSchema,
                                                   SchemaPtr outputSchema,
                                                   Runtime::Execution::ExecutablePipelineStagePtr executablePipelineStage)
    : Operator(id), PhysicalUnaryOperator(id, statisticId, std::move(inputSchema), std::move(outputSchema)),
      executablePipelineStage(std::move(executablePipelineStage)) {}

PhysicalOperatorPtr
PhysicalExternalOperator::create(StatisticId statisticId,
                                 const SchemaPtr& inputSchema,
                                 const SchemaPtr& outputSchema,
                                 const Runtime::Execution::ExecutablePipelineStagePtr& executablePipelineStage) {
    return create(getNextOperatorId(), statisticId, inputSchema, outputSchema, executablePipelineStage);
}
PhysicalOperatorPtr
PhysicalExternalOperator::create(OperatorId id,
                                 StatisticId statisticId,
                                 const SchemaPtr& inputSchema,
                                 const SchemaPtr& outputSchema,
                                 const Runtime::Execution::ExecutablePipelineStagePtr& executablePipelineStage) {
    return std::make_shared<PhysicalExternalOperator>(id, statisticId, inputSchema, outputSchema, executablePipelineStage);
}

std::string PhysicalExternalOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalExternalOperator:\n";
    out << PhysicalUnaryOperator::toString();
    out << executablePipelineStage->getCodeAsString();
    out << std::endl;
    return out.str();
}

OperatorPtr PhysicalExternalOperator::copy() {
    auto result = create(id, statisticId, inputSchema, outputSchema, executablePipelineStage);
    result->addAllProperties(properties);
    return result;
}

Runtime::Execution::ExecutablePipelineStagePtr PhysicalExternalOperator::getExecutablePipelineStage() {
    return executablePipelineStage;
}

}// namespace NES::QueryCompilation::PhysicalOperators
