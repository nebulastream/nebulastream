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

#pragma once

class TupleOutOfOrderMakerOperatorHandler {
public:

    void createEmitIndicesForInputbuffer(uint64_t numberOfTuples) {
       /// Here we create the output indices somewhat randomly, so that we can have x% random tuples in the output buffer
       /// x% of random tuples means that for 100 tuples I will have 100-x tuples that will be in order and x tuples that will be out of order
    }

    /// Stores at position i the output position of the tuple at position i in the input buffer
    /// emitIndices[i] = j means that the tuple at position i in the input buffer will be at position j in the output buffer
    std::vector<uint64_t> emitIndices;
}

class TupleOutOfOrderMaker {

    public:
        void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) {
               /// Calling TupleOutOfOrderMakerOperatorHandler so that we can create in C++-runtime the positions of the output tuples for the ocrresponding input tuples

           /// Iterating over all tuples and writing the read record to the output buffer at the position specified by emitIndices
            auto numberOfRecords = recordBuffer.getNumRecords();
            auto resultBuffer = RecordBuffer(executionCtx.allocateBuffer());
            auto resultTupleBuffer = resultBuffer.getBuffer();
            for (nautilus::val<uint64_t> i = 0_u64; i < numberOfRecords; i = i + 1_u64)
            {
                auto record = memoryProvider->readRecord(projections, recordBuffer, i);
                const auto outputIndex = nautilus::invoke(+[](const uint64_t i, OperatorHandler* opHandler) {
                    return dynamic_cast<TupleOutOfOrderMakerOperatorHandler*>(opHandler)->emitIndices[i];
                }, i, executionCtx.getGlobalOperatorHandler(operatorHandlerIndex));
                memoryProvider->writeRecord(outputIndex, resultBuffer, curRecord);
            }
        }

    private:
    uint64_t operatorHandlerIndex;
    uint64_t percentageOfRandomTuples;


};
