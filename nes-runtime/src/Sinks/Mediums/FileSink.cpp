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
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TimeMeasurement.hpp>
#include <arpa/inet.h>
#include <filesystem>
#include <iostream>
#include <netinet/in.h>
#include <regex>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <utility>

namespace NES {

SinkMediumTypes FileSink::getSinkMediumType() { return SinkMediumTypes::FILE_SINK; }

FileSink::FileSink(SinkFormatPtr format,
                   Runtime::NodeEnginePtr nodeEngine,
                   uint32_t numOfProducers,
                   const std::string& filePath,
                   bool append,
                   SharedQueryId sharedQueryId,
                   DecomposedQueryId decomposedQueryId,
                   DecomposedQueryPlanVersion decomposedQueryVersion,
                   uint64_t numberOfOrigins,
                   bool timestampAndWriteToSocket)
    : SinkMedium(std::move(format),
                 std::move(nodeEngine),
                 numOfProducers,
                 sharedQueryId,
                 decomposedQueryId,
                 decomposedQueryVersion,
                 numberOfOrigins),
      filePath(filePath), append(append), timestampAndWriteToSocket(true) {
    (void) timestampAndWriteToSocket;
}

std::string FileSink::toString() const {
    std::stringstream ss;
    ss << "FileSink(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << ")";
    ss << ")";
    return ss.str();
}

void FileSink::setup() {
    NES_DEBUG("Setting up file sink; filePath={}, schema={}, sinkFormat={}, append={}",
              filePath,
              sinkFormat->getSchemaPtr()->toString(),
              sinkFormat->toString(),
              append);
    // Remove an existing file unless the append mode is APPEND.
    if (!append) {
        if (std::filesystem::exists(filePath.c_str())) {
            std::error_code ec;
            if (!std::filesystem::remove(filePath.c_str(), ec)) {
                NES_ERROR("Could not remove existing output file: filePath={} ", filePath);
                isOpen = false;
                return;
            }
        }
    } else {
        if (std::filesystem::exists(filePath.c_str())) {
            // Skip writing the schema to the file as we are appending to an existing file.
            schemaWritten = true;
        }
    }

    if (!timestampAndWriteToSocket) {
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

    } else {

        //todo: use this in case domain socket should be used instead of tcp
        //    // Create a Unix domain socket
        //    clientSockFd = socket(AF_UNIX, SOCK_STREAM, 0);
        //    if (clientSockFd == -1) {
        //        perror("socket");
        //        return;
        //    }
        //
        //    // Set up the address structure
        //    struct sockaddr_un addr;
        //    memset(&addr, 0, sizeof(addr));
        //    addr.sun_family = AF_UNIX;
        //    strncpy(addr.sun_path, filePath.c_str(), sizeof(addr.sun_path) - 1);
        //
        //    // Connect to the Unix domain socket
        //    if (connect(clientSockFd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        //        perror("connect");
        //        close(clientSockFd);
        //        return;
        //    }

        NES_DEBUG("Connecting to tcp socket on worker {}", nodeEngine->getNodeId());
        //todo: this will not work for multiple sinks
        //    if (!this->nodeEngine->getTcpDescriptor(filePath).has_value()) {
        ////    if (true) {
        //    } else {
        //        sockfd = this->nodeEngine->getTcpDescriptor(filePath).value();
        //        NES_ERROR("Found existing tcp descriptor {} for {}", sockfd, filePath)
        //    }
        //get it once
        NES_DEBUG("creating socket for file sink")
        auto sockfd = nodeEngine->getTcpDescriptor(filePath);
    }
}

void FileSink::shutdown() {
    if (timestampAndWriteToSocket) {
        //todo: send checkpoint message
        NES_DEBUG("sink notifies checkpoints");
        //        auto sinkInfo = nodeEngine->getTcpDescriptor(filePath);
        //        auto checkpoints = sinkInfo->checkpoints;

        if (getReplayData()) {
            auto nodeEngine = this->nodeEngine;
//            auto filePath = this->filePath;
            auto sharedQueryId = this->sharedQueryId;
            auto checkpoints = nodeEngine->writeLockSeqQueue(filePath);
            std::unordered_map<uint64_t, uint64_t> map;
            for (auto& [id, point] : *checkpoints) {
                NES_DEBUG("sending checkpoint {} {}", id, point.getCurrentValue());
                map.insert({id, point.getCurrentValue()});
            }
            std::thread thread([nodeEngine,sharedQueryId, map]() mutable {
                //                auto checkpoints = nodeEngine->getLastWrittenCopy(filePath);
                nodeEngine->notifyCheckpoints(sharedQueryId, map);
            });
            thread.detach();
        }
        NES_DEBUG("notify checkpoint created");
        //NES_INFO("total buffers received at file sink {}", totalTupleCountreceived);
        //        for (const auto& bufferContent : receivedBuffers) {
        //            outputFile.write(bufferContent.c_str(), bufferContent.size());
        //        }
        //outputFile.flush();
    }
}

bool FileSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef context) {
    (void) context;
    auto sinkInfo = nodeEngine->getTcpDescriptor(filePath);

    NES_DEBUG("got buffer {}", inputBuffer.getSequenceNumber());
    if (!getReplayData()) {
        NES_DEBUG("replay data not activated writing buffer");
        std::vector<Runtime::TupleBuffer> vec = {inputBuffer};
        return writeDataToTCP(vec, sinkInfo);
    }

    // Stop execution if the file could not be opened during setup.
    // This results in ExecutionResult::Error for the task.
    numberOfReceivedBuffers++;

    if (!inputBuffer) {
        NES_ERROR("Invalid input buffer");
        return false;
    }

    //todo: get source id
    //todo: check if we can use origin id
    NES_ASSERT(inputBuffer.getNumberOfTuples() != 0, "number of tuples must not be 0");
    auto sourceId = ((Record*) inputBuffer.getBuffer())->id;
    NES_DEBUG("writing data for source id {}", sourceId);
    const auto bufferSeqNumber = inputBuffer.getSequenceNumber();
    auto seqQueueMap = nodeEngine->writeLockSeqQueue(filePath);
    auto bufferStorage = nodeEngine->writeLockSinkStorage(filePath);
    {

        if (!seqQueueMap->contains(sourceId)) {
            Sequencing::NonBlockingMonotonicSeqQueue<uint64_t> newQueue;
            seqQueueMap->operator[](sourceId) = newQueue;
            NES_DEBUG("creating new queue with start {}", seqQueueMap->at(sourceId).getCurrentValue());
        }
    }

    auto currentSeqNumberBeforeAdding = seqQueueMap->at(sourceId).getCurrentValue();

    //ignore this buffer if it was already written
    if (bufferSeqNumber <= currentSeqNumberBeforeAdding) {
        NES_DEBUG("seq {} is less than {} for id {}", bufferSeqNumber, currentSeqNumberBeforeAdding, sourceId);
        return true;
    }

    {
        if (bufferStorage->operator[](sourceId).contains(bufferSeqNumber)) {
            NES_DEBUG("buffer already recorded, exiting");
            return true;
        }
        NES_DEBUG("buffer not yet recored, proceeedign");
        bufferStorage->operator[](sourceId).emplace(bufferSeqNumber);
    }
    NES_DEBUG("increase seq nr")

    // save the highest consecutive sequence number in the queue

    // create sequence data without chunks, so chunk number is 1 and last chunk flag is true
    const auto seqData = SequenceData(bufferSeqNumber, 1, true);
    // insert input buffer sequence number to the queue
    seqQueueMap->at(sourceId).emplace(seqData, bufferSeqNumber);

    // get the highest consecutive sequence number in the queue after adding new value
    auto currentSeqNumberAfterAdding = seqQueueMap->at(sourceId).getCurrentValue();

    NES_DEBUG("seq number before adding {}, after adding {}, source id {}, sharedQueryId {}",
              currentSeqNumberBeforeAdding,
              currentSeqNumberAfterAdding,
              sourceId,
              sharedQueryId);
    // check if top value in the queue has changed after adding new sequence number
    NES_DEBUG("writing data: seq number before adding {}, after adding {}, source id {}, sharedQueryId {}",
              currentSeqNumberBeforeAdding,
              currentSeqNumberAfterAdding,
              sourceId,
              sharedQueryId);
    //        auto bufferStorageLocked = bufferStorage.wlock();
    NES_DEBUG("erasing elements");
    bufferStorage->operator[](sourceId).erase(bufferStorage->operator[](sourceId).begin(),
                                            bufferStorage->operator[](sourceId).find(currentSeqNumberAfterAdding));
    NES_DEBUG("returning")
    std::vector<Runtime::TupleBuffer> vec = {inputBuffer};
    return writeDataToTCP(vec, sinkInfo);
}

//bool FileSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef context) {
//    (void) context;
//
//    NES_DEBUG("got buffer {}", inputBuffer.getSequenceNumber());
//    if (!getReplayData()) {
//        NES_DEBUG("replay data not activated writing buffer");
//        std::vector<Runtime::TupleBuffer> vec = {inputBuffer};
//        return writeDataToTCP(vec);
//    }
//
//    // Stop execution if the file could not be opened during setup.
//    // This results in ExecutionResult::Error for the task.
//    numberOfReceivedBuffers++;
//
//    //    if (!isOpen) {
//    //        NES_DEBUG("The output file could not be opened during setup of the file sink.");
//    //        return false;
//    //    }
//
//    if (!inputBuffer) {
//        NES_DEBUG("Invalid input buffer");
//        return false;
//    }
//
//    //todo: get source id
//    //todo: check if we can use origin id
//    NES_ASSERT(inputBuffer.getNumberOfTuples() != 0, "number of tuples must not be 0");
//    auto sourceId = ((Record*) inputBuffer.getBuffer())->id;
//    NES_DEBUG("writing data for source id {}", sourceId);
//    const auto bufferSeqNumber = inputBuffer.getSequenceNumber();
//
//    {
//        auto lastWrittenMap = nodeEngine->lockLastWritten(filePath);
//
//        uint64_t lastWritten = 0;
//        if (lastWrittenMap->contains(sourceId)) {
//            lastWritten = lastWrittenMap->at(sourceId);
//        }
//
//        //ignore this buffer if it was already written
//        if (bufferSeqNumber <= lastWritten) {
//            return true;
//        }
//
//        if (!seqQueueMap.rlock()->contains(sourceId)) {
//            auto readLock = seqQueueMap.wlock();
//            if (!readLock->contains(sourceId)) {
//
//                Sequencing::NonBlockingMonotonicSeqQueue<uint64_t> newQueue;
//                //todo: fixme
//                for (uint64_t i = 0; i <= lastWritten; ++i) {
//                    const auto seqData = SequenceData(i, 1, true);
//                    newQueue.emplace(seqData, i);
//                }
//                readLock->operator[](sourceId) = newQueue;
//                NES_DEBUG("creating new queue with start {}", readLock->at(sourceId).getCurrentValue());
//            }
//        }
//    }
//
//    // get sequence number of received buffer
//    //    auto bufferCopy = context.getUnpooledBuffer(inputBuffer.getBufferSize());
//    //    std::memcpy(bufferCopy.getBuffer(), inputBuffer.getBuffer(), inputBuffer.getBufferSize());
//    //    bufferCopy.setNumberOfTuples(inputBuffer.getNumberOfTuples());
//    //    bufferCopy.setOriginId(inputBuffer.getOriginId());
//    //    bufferCopy.setWatermark(inputBuffer.getWatermark());
//    //    bufferCopy.setCreationTimestampInMS(inputBuffer.getCreationTimestampInMS());
//    //    bufferCopy.setSequenceNumber(inputBuffer.getSequenceNumber());
//    //    inputBuffer.release();
//    {
//        //        bufferStorage.wlock()->operator[](sourceId).emplace(bufferSeqNumber, bufferCopy);
//        if (bufferStorage.wlock()->operator[](sourceId).contains(bufferSeqNumber)) {
//            NES_DEBUG("buffer already recorded, exiting");
//            return true;
//        }
//        auto records = (Record*) inputBuffer.getBuffer();
//        std::vector<Record> vector;
//        for (uint64_t i = 0; i < inputBuffer.getNumberOfTuples(); ++i) {
//            records[i].outputTimestamp = getTimestamp();
//            vector.push_back(records[i]);
//        }
//        bufferStorage.wlock()->operator[](sourceId).emplace(bufferSeqNumber, std::move(vector));
//    }
//
//    // save the highest consecutive sequence number in the queue
//
//    //todo: there is a problem with locking here
//    auto currentSeqNumberBeforeAdding = seqQueueMap.rlock()->at(sourceId).getCurrentValue();
//
//    // create sequence data without chunks, so chunk number is 1 and last chunk flag is true
//    const auto seqData = SequenceData(bufferSeqNumber, 1, true);
//    // insert input buffer sequence number to the queue
//    seqQueueMap.wlock()->at(sourceId).emplace(seqData, bufferSeqNumber);
//
//    // get the highest consecutive sequence number in the queue after adding new value
//    auto currentSeqNumberAfterAdding = seqQueueMap.rlock()->at(sourceId).getCurrentValue();
//
//    NES_DEBUG("seq number before adding {}, after adding {}, source id {}, sharedQueryId {}", currentSeqNumberBeforeAdding, currentSeqNumberAfterAdding, sourceId, sharedQueryId);
//    // check if top value in the queue has changed after adding new sequence number
//    //    if (currentSeqNumberBeforeAdding != currentSeqNumberAfterAdding) {
//    NES_DEBUG("try to acquire lock for written map")
//    auto lastWrittenMap = nodeEngine->tryLockLastWritten(filePath);
//    if (lastWrittenMap) {
//        NES_DEBUG("got lock for written map")
//        //        std::vector<Runtime::TupleBuffer> bulkWriteBatch;
//        std::vector<std::vector<Record>> bulkWriteBatch;
//
//        if (!lastWrittenMap->contains(sourceId)) {
//            lastWrittenMap->insert({sourceId, 0});
//        };
//        auto& lastWritten = lastWrittenMap->at(sourceId);
//
//        if (lastWritten < currentSeqNumberAfterAdding) {
//            {
//                NES_DEBUG("writing data: last written {} seq number before adding {}, after adding {}, source id {}, sharedQueryId {}", lastWritten, currentSeqNumberBeforeAdding, currentSeqNumberAfterAdding, sourceId, sharedQueryId);
//                auto bufferStorageLocked = bufferStorage.wlock();
//                //todo: reduce hashmap lookups
//                //            for (auto it = bufferStorageLocked->operator[](sourceId).lower_bound(lastWritten + 1);
//                //                 it != bufferStorageLocked->operator[](sourceId).upper_bound(currentSeqNumberAfterAdding);
//                //                 ++it) {
//                for (auto it = bufferStorageLocked->operator[](sourceId).find(lastWritten + 1);
//                     it != bufferStorageLocked->operator[](sourceId).upper_bound(currentSeqNumberAfterAdding);
//                     ++it) {
//                    NES_DEBUG("adding element with id {}", it->second.front().id);
//                    bulkWriteBatch.push_back(it->second);// Collect the value (TupleBuffer) into the batch
//                }
//                NES_DEBUG("erasing elements");
//                bufferStorageLocked->operator[](sourceId).erase(
//                    bufferStorageLocked->operator[](sourceId).find(lastWritten + 1),
//                    bufferStorageLocked->operator[](sourceId).upper_bound(currentSeqNumberAfterAdding));
//            }
//
//            //TODO: take care of cleanup
//
//            NES_DEBUG("writing data to tcp")
//
//            // Write all buffers at once
//            writeDataToTCP(bulkWriteBatch);
//
//            NES_DEBUG("wrote data to tcp updatign last written count")
//            // NES_ERROR("wrote from {} to {}", lastWritten + 1, currentSeqNumberAfterAdding);
//
//            // Update lastWritten to the latest sequence number
//            lastWritten = currentSeqNumberAfterAdding;
//        }
//    }
//    //    }
//
//    //todo: check if we need this
//    //    if (numberOfReceivedBuffers == inputBuffer.getWatermark() && numberOfWrittenBuffers != numberOfReceivedBuffers) {
//    //        lastWrittenMtx.lock();
//    //        auto bufferStorageLocked = *bufferStorage.rlock();
//    //        // write all tuple buffers with sequence numbers in (lastWritten; currentSeqNumberAfterAdding]
//    //        std::vector<Runtime::TupleBuffer> bulkWriteBatch;
//    //
//    //        // Iterate through the map from (lastWritten + 1) to (currentSeqNumberAfterAdding)
//    //        for (auto it = bufferStorageLocked.lower_bound(lastWritten + 1);
//    //             it != bufferStorageLocked.upper_bound(inputBuffer.getWatermark());
//    //             ++it) {
//    //            bulkWriteBatch.push_back(it->second);  // Collect the value (TupleBuffer) into the batch
//    //        }
//    //
//    //        // Write all buffers at once
//    //        writeDataToTCP(bulkWriteBatch);
//    //
//    //        // Update lastWritten to the latest sequence number
//    //        lastWritten = inputBuffer.getWatermark();
//    //        lastWrittenMtx.unlock();
//    //    }
//
//    NES_DEBUG("returning")
//    return true;
//}

//bool FileSink::writeDataToTCP(std::vector<std::vector<Record>>& buffersToWrite) {
//
//    auto sinkInfo = nodeEngine->getTcpDescriptor(filePath);
//    if (timestampAndWriteToSocket) {
//        NES_DEBUG("write data to sink with descriptor {} for {}", sinkInfo->sockfd, filePath)
//
//        for (auto records : buffersToWrite) {
//            ssize_t bytes_written = write(sinkInfo->sockfd, records.data(), records.size() * sizeof(Record));
//            //        NES_ERROR("{} bytes written to tcp sink", bytes_written);
//            if (bytes_written == -1) {
//                NES_ERROR("Could not write to socket in sink")
//                perror("write");
//                close(sinkInfo->sockfd);
//                return 1;
//            }
//
//            //        receivedBuffers.push_back(bufferContent);
//            //        arrivalTimestamps.push_back(getTimestamp());
//        }
//        NES_DEBUG("finished writing to sink with descriptor {} for {}", sinkInfo->sockfd, filePath)
//        return true;
//    }
//    return false;
//}

bool FileSink::writeDataToTCP(std::vector<Runtime::TupleBuffer>& buffersToWrite,
                              folly::Synchronized<Runtime::TCPSinkInfo>::LockedPtr& sinkInfo) {

    //    auto sinkInfo = nodeEngine->getTcpDescriptor(filePath);
    if (timestampAndWriteToSocket) {
        NES_DEBUG("write data to sink with descriptor {} for {}", sinkInfo->sockfd, filePath)
        //        std::string bufferContent;
        //auto schema = sinkFormat->getSchemaPtr();
        //schema->removeField(AttributeField::create("timestamp", DataTypeFactory::createType(BasicType::UINT64)));
        //bufferContent = Util::printTupleBufferAsCSV(inputBuffer, schema, timestampAndWriteToSocket);
        //totalTupleCountreceived += inputBuffer.getNumberOfTuples();

        // Write data to the socket
        //ssize_t bytes_written = write(sockfd, bufferContent.c_str(), bufferContent.length());

        for (auto bufferToWrite : buffersToWrite) {

            auto* records = bufferToWrite.getBuffer<Record>();
            for (uint64_t i = 0; i < bufferToWrite.getNumberOfTuples(); ++i) {
                records[i].outputTimestamp = getTimestamp();
            }
            //        NES_ERROR("Writing to tcp sink");
            ssize_t bytes_written =
                write(sinkInfo->sockfd, bufferToWrite.getBuffer(), bufferToWrite.getNumberOfTuples() * sizeof(Record));
            //        NES_ERROR("{} bytes written to tcp sink", bytes_written);
            if (bytes_written == -1) {
                NES_ERROR("Could not write to socket in sink")
                perror("write");
                close(sinkInfo->sockfd);
                return 1;
            }

            //        receivedBuffers.push_back(bufferContent);
            //        arrivalTimestamps.push_back(getTimestamp());
        }
        NES_DEBUG("finished writing to sink with descriptor {} for {}", sinkInfo->sockfd, filePath)
        return true;
    }
    return false;
}

//bool FileSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
//    // auto sockfd = nodeEngine->getTcpDescriptor(filePath);
//    auto sinkInfo = nodeEngine->getTcpDescriptor(filePath);
//    if (timestampAndWriteToSocket) {
//        NES_DEBUG("write data to sink with descriptor {} for {}", sinkInfo->sockfd, filePath)
//        std::unique_lock lock(writeMutex);
////        std::string bufferContent;
//        //auto schema = sinkFormat->getSchemaPtr();
//        //schema->removeField(AttributeField::create("timestamp", DataTypeFactory::createType(BasicType::UINT64)));
//        //bufferContent = Util::printTupleBufferAsCSV(inputBuffer, schema, timestampAndWriteToSocket);
//        //totalTupleCountreceived += inputBuffer.getNumberOfTuples();
//
//
//        // Write data to the socket
//        //ssize_t bytes_written = write(sockfd, bufferContent.c_str(), bufferContent.length());
//
//        auto* records = inputBuffer.getBuffer<Record>();
//        //todo: do not checkpoint if incremental is enabled
//        for (uint64_t i = 0; i < inputBuffer.getNumberOfTuples(); ++i) {
//            records[i].outputTimestamp = getTimestamp();
//            auto& checkpoint = sinkInfo->checkpoints[records[i].id];
////            if (checkpoint != 0 && checkpoint >= records[i].value) {
////                NES_ERROR("checkpoint is {}, tuple is {}. Replay cannot work with out of orderness", checkpoint, records[i].value);
////                NES_ASSERT(false, "Checkpoints not increasing");
////            }
//            if (records[i].value == 0 || checkpoint == records[i].value + 1) {
//                checkpoint = records[i].value;
//            }
//        }
////        NES_ERROR("Writing to tcp sink");
//        ssize_t bytes_written = write(sinkInfo->sockfd, inputBuffer.getBuffer(), inputBuffer.getNumberOfTuples() * sizeof(Record));
////        NES_ERROR("{} bytes written to tcp sink", bytes_written);
//        if (bytes_written == -1) {
//            NES_ERROR("Could not write to socket in sink")
//            perror("write");
//            close(sinkInfo->sockfd);
//            return 1;
//        }
//
//        //        receivedBuffers.push_back(bufferContent);
//        //        arrivalTimestamps.push_back(getTimestamp());
//
//        NES_DEBUG("finished writing to sink with descriptor {} for {}", sinkInfo->sockfd, filePath)
//        return true;
//    }
//    return writeDataToFile(inputBuffer);
//}

std::string FileSink::getFilePath() const { return filePath; }

bool FileSink::writeDataToFile(Runtime::TupleBuffer& inputBuffer) {
    std::unique_lock lock(writeMutex);

    if (!inputBuffer) {
        NES_DEBUG("Invalid input buffer");
        return false;
    }

    if (!schemaWritten && sinkFormat->getSinkFormat() != FormatTypes::NES_FORMAT) {
        NES_DEBUG("Writing schema to file sink; filePath = {}, schema = {}, sinkFormat = {}",
                  filePath,
                  sinkFormat->getSchemaPtr()->toString(),
                  sinkFormat->toString());
        auto schemaStr = sinkFormat->getFormattedSchema();
        outputFile.write(schemaStr.c_str(), (int64_t) schemaStr.length());
        schemaWritten = true;
    } else if (sinkFormat->getSinkFormat() == FormatTypes::NES_FORMAT) {
        NES_DEBUG("Writing the schema is not supported for NES_FORMAT");
    } else {
        NES_DEBUG("Schema already written");
    }

    auto fBuffer = sinkFormat->getFormattedBuffer(inputBuffer);
    NES_DEBUG("Writing tuples to file sink; decomposed query id={} filePath={}, fBuffer={}",
              decomposedQueryId,
              filePath,
              fBuffer);
    outputFile.write(fBuffer.c_str(), fBuffer.size());
    outputFile.flush();
    return true;
}

}// namespace NES
