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

#include <Execution/Operators/ReorderTupleBuffersOperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators {
ReorderTupleBuffersOperatorHandler::ReorderTupleBuffersOperatorHandler() {
    // default value must be 0, as first buffer to be written should be with index 1
    lastWritten = 0;
};

void ReorderTupleBuffersOperatorHandler::processBuffer(PipelineExecutionContext* ctx,
                                                       WorkerContext& wc,
                                                       TupleBuffer& inputBuffer) {

    NES_DEBUG("got new buffer {}", inputBuffer.getSequenceNumber());
    // get sequence number of received buffer
    const auto bufferSeqNumber = inputBuffer.getSequenceNumber();
    bufferStorage.wlock()->emplace(bufferSeqNumber, inputBuffer);
    // save the highest consecutive sequence number in the queue
    auto currentSeqNumberBeforeAdding = seqQueue.getCurrentValue();

    // create sequence data without chunks, so chunk number is 1 and last chunk flag is true
    const auto seqData = SequenceData(bufferSeqNumber, 1, true);
    // insert input buffer sequence number to the queue
    seqQueue.emplace(seqData, bufferSeqNumber);

    // get the highest consecutive sequence number in the queue after adding new value
    auto currentSeqNumberAfterAdding = seqQueue.getCurrentValue();

    // TODO: #5033 check this logic
    // check if top value in the queue has changed after adding new sequence number
    if (currentSeqNumberBeforeAdding != currentSeqNumberAfterAdding) {
        // write all tuple buffers with sequence numbers in (lastWritten; currentSeqNumberAfterAdding]
        auto wLock = lastWritten.tryWLock();
        if (wLock) {
            while ((*wLock + 1) <= seqQueue.getCurrentValue()) {
                auto bufferStorageLocked = *bufferStorage.rlock();
                // get tuple buffer with next sequence number after lastWritten and update lastWritten
                auto nextTupleBufferToBeEmitted = bufferStorageLocked[++*wLock];
                // emit next tuple buffer
                // NOTE: emit buffer must be called, as dispatch buffer will put buffer to the task queue and won't guarantee the order
                ctx->emitBuffer(nextTupleBufferToBeEmitted, wc);
                // delete emitted tuple buffer from storage
                bufferStorage.wlock()->erase(*wLock);
            }
        }
    }

//    if (inputBuffer.getSequenceNumber() == inputBuffer.getWatermark()) {
//        auto wLock = lastWritten.wlock();
//        while ((*wLock + 1) <= seqQueue.getCurrentValue()) {
//            auto bufferStorageLocked = *bufferStorage.rlock();
//            // get tuple buffer with next sequence number after lastWritten and update lastWritten
//            auto nextTupleBufferToBeEmitted = bufferStorageLocked[++*wLock];
//            // emit next tuple buffer
//            // NOTE: emit buffer must be called, as dispatch buffer will put buffer to the task queue and won't guarantee the order
//            ctx->emitBuffer(nextTupleBufferToBeEmitted, wc);
//            // delete emitted tuple buffer from storage
//            bufferStorage.wlock()->erase(*wLock);
//        }
//    }
};
}// namespace NES::Runtime::Execution::Operators
