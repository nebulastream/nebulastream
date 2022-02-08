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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalBatchJoinBuildOperator.hpp>
#include <utility>

#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Windowing/WindowHandler/BatchJoinOperatorHandler.hpp>
#include <Windowing/WindowHandler/BatchJoinHandler.hpp>


// todo jm delete as it is not used anymore
class CustomPipelineStageBatchJoinBuild : public NES::Runtime::Execution::ExecutablePipelineStage {

    struct __attribute__((packed)) InputBuildTuple {
        int64_t build$id1;
        int64_t build$value;
    };

    NES::ExecutionResult execute(
            NES::Runtime::TupleBuffer &inputTupleBuffer,
            NES::Runtime::Execution::PipelineExecutionContext &pipelineExecutionContext,
            __attribute__((unused)) NES::Runtime::WorkerContext &workerContext) {
        /* variable declarations */
        NES::ExecutionResult ret = NES::ExecutionResult::Ok;
        /* statements section */
        InputBuildTuple *inputTuples = (InputBuildTuple *) inputTupleBuffer.getBuffer();
        uint64_t numberOfTuples = inputTupleBuffer.getNumberOfTuples();

        NES::Join::BatchJoinOperatorHandlerPtr joinOperatorHandler =
                pipelineExecutionContext.getOperatorHandler<NES::Join::BatchJoinOperatorHandler>(0); // <-- get index from

        NES::Join::HashTablePtr<uint64_t, InputBuildTuple> hashTable =
                joinOperatorHandler->getBatchJoinHandler<NES::Join::BatchJoinHandler, uint64_t, InputBuildTuple>()->getHashTable();

        for (uint64_t recordIndex = 0; recordIndex < numberOfTuples;
        ++recordIndex) {
            uint64_t tmp_build$id = inputTuples[recordIndex].build$id1;
            hashTable->insert({tmp_build$id, inputTuples[recordIndex]});
        };

        return ret;
        ;
    }
    uint32_t setup(
            __attribute__((unused)) NES::Runtime::Execution::PipelineExecutionContext &pipelineExecutionContext) {
        /* variable declarations */

        NES::Join::BatchJoinOperatorHandlerPtr batchJoinOperatorHandler =
                pipelineExecutionContext.getOperatorHandler<NES::Join::BatchJoinOperatorHandler>(0); // <-- get index from

        auto batchJoinDefinition = batchJoinOperatorHandler->getBatchJoinDefinition();

        NES::Join::AbstractBatchJoinHandlerPtr batchJoinHandler =
            NES::Join::BatchJoinHandler<int64_t, InputBuildTuple>::create(batchJoinDefinition, 99999);

        batchJoinOperatorHandler->setBatchJoinHandler(batchJoinHandler);

        /* statements section */
        return 0;
        ;
    }
};


namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr PhysicalBatchJoinBuildOperator::create(SchemaPtr inputSchema,
                                                      SchemaPtr outputSchema,
                                                      Join::BatchJoinOperatorHandlerPtr operatorHandler) {
    return create(Util::getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(operatorHandler));
}

PhysicalOperatorPtr PhysicalBatchJoinBuildOperator::create(OperatorId id,
                                                      const SchemaPtr& inputSchema,
                                                      const SchemaPtr& outputSchema,
                                                      const Join::BatchJoinOperatorHandlerPtr& operatorHandler) {
    return std::make_shared<PhysicalBatchJoinBuildOperator>(id, inputSchema, outputSchema, operatorHandler);
}

PhysicalBatchJoinBuildOperator::PhysicalBatchJoinBuildOperator(OperatorId id,
                                                     SchemaPtr inputSchema,
                                                     SchemaPtr outputSchema,
                                                     Join::BatchJoinOperatorHandlerPtr operatorHandler)
    : OperatorNode(id), PhysicalBatchJoinOperator(std::move(operatorHandler)),
      PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)) {};

std::string PhysicalBatchJoinBuildOperator::toString() const { return "PhysicalBatchJoinBuildOperator"; }

OperatorNodePtr PhysicalBatchJoinBuildOperator::copy() {
    return create(id, inputSchema, outputSchema, operatorHandler);
}

// todo jm delete as it is not used anymore
Runtime::Execution::ExecutablePipelineStagePtr PhysicalBatchJoinBuildOperator::getExecutablePipelineStage() {
    return std::make_shared<CustomPipelineStageBatchJoinBuild>();
}

}// namespace NES::QueryCompilation::PhysicalOperators