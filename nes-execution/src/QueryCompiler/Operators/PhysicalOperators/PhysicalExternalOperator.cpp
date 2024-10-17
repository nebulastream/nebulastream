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
#include <sstream>
#include <utility>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalExternalOperator.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalExternalOperator::PhysicalExternalOperator(
    OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Runtime::Execution::ExecutablePipelineStagePtr executablePipelineStage)
    : Operator(id)
    , PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema))
    , executablePipelineStage(std::move(executablePipelineStage))
{
}

PhysicalOperatorPtr PhysicalExternalOperator::create(
    const SchemaPtr& inputSchema,
    const SchemaPtr& outputSchema,
    const Runtime::Execution::ExecutablePipelineStagePtr& executablePipelineStage)
{
    return create(getNextOperatorId(), inputSchema, outputSchema, executablePipelineStage);
}
PhysicalOperatorPtr PhysicalExternalOperator::create(
    OperatorId id,
    const SchemaPtr& inputSchema,
    const SchemaPtr& outputSchema,
    const Runtime::Execution::ExecutablePipelineStagePtr& executablePipelineStage)
{
    return std::make_shared<PhysicalExternalOperator>(id, inputSchema, outputSchema, executablePipelineStage);
}

std::string PhysicalExternalOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalExternalOperator:\n";
    out << PhysicalUnaryOperator::toString();
    out << std::endl;
    return out.str();
}

OperatorPtr PhysicalExternalOperator::copy()
{
    auto result = create(id, inputSchema, outputSchema, executablePipelineStage);
    result->addAllProperties(properties);
    return result;
}

Runtime::Execution::ExecutablePipelineStagePtr PhysicalExternalOperator::getExecutablePipelineStage()
{
    return executablePipelineStage;
}

}
