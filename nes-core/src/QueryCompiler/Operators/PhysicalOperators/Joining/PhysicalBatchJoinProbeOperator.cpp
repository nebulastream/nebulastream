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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalBatchJoinProbeOperator.hpp>
#include <utility>

#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Windowing/WindowHandler/BatchJoinHandler.hpp>
#include <Windowing/WindowHandler/BatchJoinOperatorHandler.hpp>


class CustomPipelineStageBatchJoinProbe : public NES::Runtime::Execution::ExecutablePipelineStage {
    struct __attribute__((packed)) InputProbeTuple {
        int64_t probe$id;
        int64_t probe$one;
        int64_t probe$value;
    };
    struct __attribute__((packed)) InputBuildTuple {
        int64_t build$id;
        int64_t build$value;
    };
    struct __attribute__((packed)) ResultTuple {
        int64_t probe$id;
        int64_t probe$one;
        int64_t probe$value;

        int64_t build$id;
        int64_t build$value;
    };

    NES::SchemaPtr outputSchema = NES::Schema::create()
            ->addField("probe$id", NES::BasicType::INT64)
            ->addField("probe$one", NES::BasicType::INT64)
            ->addField("probe$value", NES::BasicType::INT64)
            ->addField("build$id", NES::BasicType::INT64)
            ->addField("build$value", NES::BasicType::INT64);


    NES::ExecutionResult execute(
            NES::Runtime::TupleBuffer &inputTupleBuffer,
            NES::Runtime::Execution::PipelineExecutionContext &pipelineExecutionContext,
            __attribute__((unused)) NES::Runtime::WorkerContext &workerContext) {
        /* variable declarations */
        NES::ExecutionResult ret = NES::ExecutionResult::Ok;
        int64_t numberOfResultTuples = 0;
        /* statements section */
        InputProbeTuple *inputTuples = (InputProbeTuple *) inputTupleBuffer.getBuffer();
        uint64_t numberOfTuples = inputTupleBuffer.getNumberOfTuples();
        NES::Runtime::TupleBuffer resultTupleBuffer = workerContext.allocateTupleBuffer();;
        ResultTuple *resultTuples = (ResultTuple *) resultTupleBuffer.getBuffer();

        uint64_t maxTuple=resultTupleBuffer.getBufferSize()/sizeof(ResultTuple);


        NES::Join::BatchJoinOperatorHandlerPtr joinOperatorHandler =
                pipelineExecutionContext.getOperatorHandler<NES::Join::BatchJoinOperatorHandler>(0); // <-- get index from

        NES::Join::HashTablePtr<uint64_t, InputBuildTuple> hashTable =
                joinOperatorHandler->getBatchJoinHandler<NES::Join::BatchJoinHandler, uint64_t, InputBuildTuple>()->getHashTable();

        NES_DEBUG("New call of PROBE pipeline. numberOfTuples: " << numberOfTuples);

        for (uint64_t recordIndex = 0; recordIndex < numberOfTuples;
        ++recordIndex) {
            int64_t tmp_probe$id = inputTuples[recordIndex].probe$id;
            int64_t tmp_probe$one = inputTuples[recordIndex].probe$one;
            int64_t tmp_probe$value = inputTuples[recordIndex].probe$value;

            auto rangeJoinable = hashTable->equal_range(tmp_probe$id);
            size_t numJoinable = std::distance(rangeJoinable.first, rangeJoinable.second);
            if (numJoinable != 0) {
                if(numberOfResultTuples + numJoinable > maxTuple){ // check if everything will fit.
                    resultTupleBuffer.setNumberOfTuples(numberOfResultTuples);
                    resultTupleBuffer.setOriginId(inputTupleBuffer.getOriginId());
                    resultTupleBuffer.setWatermark(inputTupleBuffer.getWatermark());

                    NES_DEBUG("Buffer to emit from probe (EARLY): \n" + NES::Util::prettyPrintTupleBuffer(resultTupleBuffer, outputSchema));

                    pipelineExecutionContext.emitBuffer(resultTupleBuffer, workerContext);
                    numberOfResultTuples=0;
                    resultTupleBuffer=workerContext.allocateTupleBuffer();
                    resultTuples=(ResultTuple*)resultTupleBuffer.getBuffer();
                }
                for (auto it = rangeJoinable.first; it != rangeJoinable.second; ++it) {
                    ResultTuple resTuple = {tmp_probe$id,tmp_probe$one,tmp_probe$value,
                                            it->second.build$id, it->second.build$value};
                    resultTuples[numberOfResultTuples] = resTuple;
                    ++numberOfResultTuples;
                }
            }
        };

        resultTupleBuffer.setNumberOfTuples(numberOfResultTuples);
        resultTupleBuffer.setWatermark(inputTupleBuffer.getWatermark());
        resultTupleBuffer.setOriginId(inputTupleBuffer.getOriginId());
        resultTupleBuffer.setSequenceNumber(inputTupleBuffer.getSequenceNumber());
        NES_DEBUG("Buffer to emit from probe: \n" + NES::Util::prettyPrintTupleBuffer(resultTupleBuffer, outputSchema));
        pipelineExecutionContext.emitBuffer(resultTupleBuffer, workerContext);
        return ret;
        ;
    }
    uint32_t setup(
            __attribute__((unused)) NES::Runtime::Execution::PipelineExecutionContext &pipelineExecutionContext) {
        /* variable declarations */

        /* statements section */
        return 0;
        ;
    }

};

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr PhysicalBatchJoinProbeOperator::create(SchemaPtr leftInputSchema,
                                                     SchemaPtr rightInputSchema,
                                                     SchemaPtr outputSchema,
                                                     Join::BatchJoinOperatorHandlerPtr joinOperatorHandler) {
    return create(Util::getNextOperatorId(),
                  std::move(leftInputSchema),
                  std::move(rightInputSchema),
                  std::move(outputSchema),
                  std::move(joinOperatorHandler));
}

PhysicalOperatorPtr PhysicalBatchJoinProbeOperator::create(OperatorId id,
                                                     const SchemaPtr& leftInputSchema,
                                                     const SchemaPtr& rightInputSchema,
                                                     const SchemaPtr& outputSchema,
                                                     const Join::BatchJoinOperatorHandlerPtr& joinOperatorHandler) {
    return std::make_shared<PhysicalBatchJoinProbeOperator>(id, leftInputSchema, rightInputSchema, outputSchema, joinOperatorHandler);
}

PhysicalBatchJoinProbeOperator::PhysicalBatchJoinProbeOperator(OperatorId id,
                                                   SchemaPtr leftInputSchema,
                                                   SchemaPtr rightInputSchema,
                                                   SchemaPtr outputSchema,
                                                   Join::BatchJoinOperatorHandlerPtr joinOperatorHandler)
    : OperatorNode(id), PhysicalBatchJoinOperator(std::move(joinOperatorHandler)),
      PhysicalBinaryOperator(id, std::move(leftInputSchema), std::move(rightInputSchema), std::move(outputSchema)){};

std::string PhysicalBatchJoinProbeOperator::toString() const { return "PhysicalBatchJoinProbeOperator"; }

OperatorNodePtr PhysicalBatchJoinProbeOperator::copy() {
    return create(id, leftInputSchema, rightInputSchema, outputSchema, operatorHandler); // todo is this a valid copy? looks like we could loose the schemas and handlers at the move operator
}

Runtime::Execution::ExecutablePipelineStagePtr PhysicalBatchJoinProbeOperator::getExecutablePipelineStage() {
    return std::make_shared<CustomPipelineStageBatchJoinProbe>(); // todo
}

}// namespace NES::QueryCompilation::PhysicalOperators