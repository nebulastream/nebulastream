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

#include <Exceptions/TaskExecutionException.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Mediums/RawBufferSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <filesystem>
#include <iostream>
#include <utility>

namespace NES {

SinkMediumTypes RawBufferSink::getSinkMediumType() { return SinkMediumTypes::FILE_SINK; }

RawBufferSink::RawBufferSink(Runtime::NodeEnginePtr nodeEngine,
                             uint32_t numOfProducers,
                             const std::string& filePath,
                             bool append,
                             SharedQueryId sharedQueryId,
                             DecomposedQueryId decomposedQueryId,
                             DecomposedQueryPlanVersion decomposedQueryVersion,
                             uint64_t numberOfOrigins)
    : SinkMedium(nullptr,
                 std::move(nodeEngine),
                 numOfProducers,
                 sharedQueryId,
                 decomposedQueryId,
                 decomposedQueryVersion,
                 numberOfOrigins),
      filePath(filePath), append(append) {
    lastWritten = 0;
//    bufferStorage.reserve(1000);
}

RawBufferSink::~RawBufferSink() {}

std::string RawBufferSink::toString() const {
    std::stringstream ss;
    ss << "RawBufferSink(";
    ss << ")";
    return ss.str();
}

void RawBufferSink::setup() {
    NES_DEBUG("Setting up raw buffer sink; filePath={}, append={}", filePath, append);
    // Remove an existing file unless the append mode is APPEND.
    if (!append) {
        if (std::filesystem::exists(filePath.c_str())) {
            std::error_code ec;
            if (!std::filesystem::remove(filePath.c_str(), ec)) {
                NES_ERROR("Could not remove existing output file: filePath={}, error={}", filePath, ec.message());
                isOpen = false;
                return;
            }
        }
    }

    // Open the file stream
    if (!outputFile.is_open()) {
        outputFile.open(filePath, std::ofstream::binary | std::ofstream::app);
    }
    isOpen = outputFile.is_open() && outputFile.good();
    if (!isOpen) {
        NES_ERROR("Could not open output file; filePath={}, is_open()={}, good={}",
                  filePath,
                  outputFile.is_open(),
                  outputFile.good());
    }
}

void RawBufferSink::shutdown() {
    NES_DEBUG("Closing file sink, filePath={}", filePath);
    // rename file after dumping completed
    if (isClosed) {
        return;
    }

    auto dotPosition = filePath.find_last_of('.');
    auto completedPath = filePath.substr(0, dotPosition) + "_completed" + filePath.substr(dotPosition);
    outputFile.flush();
    outputFile.close();

    if (std::rename(filePath.c_str(), completedPath.c_str()) == 0) {
        NES_DEBUG("File successfully renamed from {}, to {}", filePath, completedPath)
    } else {
        NES_ERROR("Can't rename file after dumping");
    }
}

bool RawBufferSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    // Stop execution if the file could not be opened during setup.
    // This results in ExecutionResult::Error for the task.
    numberOfReceivedBuffers++;

    // NES_ERROR("got buffer {}", inputBuffer.getSequenceNumber());

    if (!isOpen) {
        NES_DEBUG("The output file could not be opened during setup of the file sink.");
        return false;
    }

    if (!inputBuffer) {
        NES_ERROR("Invalid input buffer");
        return false;
    }

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
        if (lastWrittenMtx.try_lock()) {
            auto bufferStorageLocked = *bufferStorage.rlock();
            while ((lastWritten + 1) <= currentSeqNumberAfterAdding) {
                // get tuple buffer with next sequence number after lastWritten and update lastWritten
                auto nextTupleBufferToBeEmitted = bufferStorageLocked[++lastWritten];
                // emit next tuple buffer
                // NOTE: emit buffer must be called, as dispatch buffer will put buffer to the task queue and won't guarantee the order
                writeToTheFile(nextTupleBufferToBeEmitted);
                // delete emitted tuple buffer from storage
                 bufferStorage.wlock()->erase(lastWritten);
            }
            lastWrittenMtx.unlock();
        }
    }

    if (numberOfReceivedBuffers == inputBuffer.getWatermark()) {
        lastWrittenMtx.lock();
        auto bufferStorageLocked = *bufferStorage.rlock();
        while ((lastWritten + 1) <= inputBuffer.getWatermark()) {
            // get tuple buffer with next sequence number after lastWritten and update lastWritten
            auto nextTupleBufferToBeEmitted = bufferStorageLocked[++lastWritten];
            // emit next tuple buffer
            // NOTE: emit buffer must be called, as dispatch buffer will put buffer to the task queue and won't guarantee the order
            writeToTheFile(nextTupleBufferToBeEmitted);
            // delete emitted tuple buffer from storage
            bufferStorage.wlock()->erase(lastWritten);
        }
        lastWrittenMtx.unlock();
    }

    return true;
}

bool RawBufferSink::writeToTheFile(Runtime::TupleBuffer& inputBuffer) {

    auto numberOfBuffers = inputBuffer.getWatermark();

    uint64_t size = inputBuffer.getBufferSize();
    uint64_t numberOfTuples = inputBuffer.getNumberOfTuples();

    // Create a temporary buffer to batch writes
    std::vector<char> buffer(sizeof(uint64_t) * 2 + size);

    // NES_ERROR("Writing tuples {} to file sink; filePath={}", inputBuffer.getSequenceNumber(), filePath);
    std::memcpy(buffer.data(), &size, sizeof(uint64_t));
    std::memcpy(buffer.data() + sizeof(uint64_t), &numberOfTuples, sizeof(uint64_t));
    std::memcpy(buffer.data() + 2 * sizeof(uint64_t), inputBuffer.getBuffer(), size);
    outputFile.write(buffer.data(), buffer.size());

    numberOfWrittenBuffers++;

    // NES_ERROR("number of written: {}", numberOfWrittenBuffers);

    if (numberOfWrittenBuffers == numberOfBuffers) {
        shutdown();
        isClosed = true;
        auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        // NES_ERROR("finished transferring {} at {}", time, numberOfWrittenBuffers);
    }
    return true;
}

bool RawBufferSink::writeBulkToFile(std::vector<Runtime::TupleBuffer>& buffers) {
    // NES_ERROR("buffers size {}", buffers.size());
    if (buffers.empty()) {
        return false;
    }

    // Precompute total size needed for bulk write
    uint64_t totalSize = 0;
    for (const auto& buf : buffers) {
        totalSize += (sizeof(uint64_t) * 2) + buf.getBufferSize();
    }

    // Allocate a single large buffer
    std::vector<char> bulkBuffer(totalSize);
    char* writePtr = bulkBuffer.data();

    // Fill the bulk buffer
    for (const auto& buf : buffers) {
        uint64_t size = buf.getBufferSize();
        uint64_t numberOfTuples = buf.getNumberOfTuples();

        std::memcpy(writePtr, &size, sizeof(uint64_t));
        writePtr += sizeof(uint64_t);

        std::memcpy(writePtr, &numberOfTuples, sizeof(uint64_t));
        writePtr += sizeof(uint64_t);

        std::memcpy(writePtr, buf.getBuffer(), size);
        writePtr += size;
    }

    // Perform a single large file write
    outputFile.write(bulkBuffer.data(), bulkBuffer.size());

    numberOfWrittenBuffers += buffers.size();

    NES_ERROR("number of written: {}", numberOfWrittenBuffers);

    // Optional: Flush or shutdown logic
    if (numberOfWrittenBuffers == buffers.front().getWatermark()) {
        shutdown();
        isClosed = true;
        auto time = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now().time_since_epoch()
                            ).count();
        NES_ERROR("Finished transferring {} at {}", time, numberOfWrittenBuffers);
    }

    return true;
}


}// namespace NES
