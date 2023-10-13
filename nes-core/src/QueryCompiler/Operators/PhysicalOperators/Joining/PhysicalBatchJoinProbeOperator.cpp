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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalBatchJoinProbeOperator.hpp>
#include <utility>

#include <Common/ExecutableType/Array.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Windowing/WindowHandler/BatchJoinHandler.hpp>
#include <Windowing/WindowHandler/BatchJoinOperatorHandler.hpp>

namespace NES::QueryCompilation::PhysicalOperators::Experimental {

PhysicalOperatorPtr PhysicalBatchJoinProbeOperator::create(SchemaPtr inputSchema,
                                                           SchemaPtr outputSchema,
                                                           Join::Experimental::BatchJoinOperatorHandlerPtr joinOperatorHandler) {
    return create(getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(joinOperatorHandler));
}

PhysicalOperatorPtr
PhysicalBatchJoinProbeOperator::create(OperatorId id,
                                       const SchemaPtr& inputSchema,
                                       const SchemaPtr& outputSchema,
                                       const Join::Experimental::BatchJoinOperatorHandlerPtr& joinOperatorHandler) {
    return std::make_shared<PhysicalBatchJoinProbeOperator>(id, inputSchema, outputSchema, joinOperatorHandler);
}

PhysicalBatchJoinProbeOperator::PhysicalBatchJoinProbeOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Join::Experimental::BatchJoinOperatorHandlerPtr joinOperatorHandler)
    : OperatorNode(id), PhysicalBatchJoinOperator(std::move(joinOperatorHandler)),
      PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)){};

std::string PhysicalBatchJoinProbeOperator::toString() const { return "PhysicalBatchJoinProbeOperator"; }

OperatorNodePtr PhysicalBatchJoinProbeOperator::copy() {
    auto result = create(id, inputSchema, outputSchema, operatorHandler);
    result->addAllProperties(properties);
    return result;
}

}// namespace NES::QueryCompilation::PhysicalOperators::Experimental