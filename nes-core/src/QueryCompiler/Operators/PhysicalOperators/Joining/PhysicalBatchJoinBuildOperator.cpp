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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalBatchJoinBuildOperator.hpp>
#include <utility>

#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Windowing/WindowHandler/BatchJoinHandler.hpp>
#include <Windowing/WindowHandler/BatchJoinOperatorHandler.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr PhysicalBatchJoinBuildOperator::create(SchemaPtr inputSchema,
                                                           SchemaPtr outputSchema,
                                                           Join::Experimental::BatchJoinOperatorHandlerPtr operatorHandler) {
    return create(getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(operatorHandler));
}

PhysicalOperatorPtr
PhysicalBatchJoinBuildOperator::create(OperatorId id,
                                       const SchemaPtr& inputSchema,
                                       const SchemaPtr& outputSchema,
                                       const Join::Experimental::BatchJoinOperatorHandlerPtr& operatorHandler) {
    return std::make_shared<PhysicalBatchJoinBuildOperator>(id, inputSchema, outputSchema, operatorHandler);
}

PhysicalBatchJoinBuildOperator::PhysicalBatchJoinBuildOperator(OperatorId id,
                                                               SchemaPtr inputSchema,
                                                               SchemaPtr outputSchema,
                                                               Join::Experimental::BatchJoinOperatorHandlerPtr operatorHandler)
    : OperatorNode(id), PhysicalBatchJoinOperator(std::move(operatorHandler)),
      PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)){};

std::string PhysicalBatchJoinBuildOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalBatchJoinBuildOperator:\n";
    out << PhysicalUnaryOperator::toString();
    return out.str();
}

OperatorNodePtr PhysicalBatchJoinBuildOperator::copy() {
    auto result = create(id, inputSchema, outputSchema, operatorHandler);
    result->addAllProperties(properties);
    return result;
}

}// namespace NES::QueryCompilation::PhysicalOperators