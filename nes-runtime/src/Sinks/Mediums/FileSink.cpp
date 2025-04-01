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
#include <numeric>

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
        NES_DEBUG("creating socket for file sink")
        //auto sockfd = nodeEngine->getTcpDescriptor(filePath);
        std::vector<OriginId> origins;
        for (uint64_t i = 1; i <= numberOfOrigins; ++i) {
            origins.emplace_back(OriginId(i));
        }
        watermarksProcessor = Windowing::MultiOriginWatermarkProcessor::create(origins);
        // Open the file stream
        //if (!outputFile.is_open()) {
        //    outputFile.open(filePath, std::ofstream::binary | std::ofstream::app);
        //}
        //isOpen = outputFile.is_open() && outputFile.good();
        //if (!isOpen) {
        //    NES_ERROR("Could not open output file; filePath={}, is_open()={}, good={}",
        //              filePath,
        //              outputFile.is_open(),
        //              outputFile.good());
        //}
    }
}

void FileSink::shutdown() {
    if (timestampAndWriteToSocket) {
        //todo: send checkpoint message
        NES_DEBUG("sink notifies checkpoints");
        if (getReplayData()) {
            auto nodeEngine = this->nodeEngine;
            auto sharedQueryId = this->sharedQueryId;
            // auto watermarkProcessor = nodeEngine->getWatermarkProcessorFor(filePath, numberOfOrigins);
            // std::unordered_map<uint64_t, uint64_t> map;
            // for (auto& [id, point] : *checkpoints) {
            auto minWatermark = watermarksProcessor->getCurrentWatermark();
            auto lastSavedWatermark = nodeEngine->getLastSavedMinWatermark(sharedQueryId);

            auto newWatermark = std::max(minWatermark, lastSavedWatermark);
            nodeEngine->updateLastSavedMinWatermark(sharedQueryId, newWatermark);
            NES_ERROR("sending checkpoint query id: {}, watermark: {}", sharedQueryId, newWatermark);
            // map.insert({sharedQueryId.getRawValue(), watermarkProcessor->get()->getCurrentWatermark()});
            // }
            std::thread thread([nodeEngine, sharedQueryId, newWatermark]() mutable {
                nodeEngine->notifyCheckpoints(sharedQueryId, newWatermark);
            });
            thread.detach();

//            if (minWatermark > lastSavedWatermark) {
                auto sinkInfo = nodeEngine->getTcpDescriptor(filePath);
                std::vector<Runtime::TupleBuffer> vec;
                NES_DEBUG("writing and erasing elements");
                for (auto& [id, bufferVec] : buffersStorage) {
                    auto it = std::remove_if(bufferVec.begin(), bufferVec.end(),
                                             [&](const Runtime::TupleBuffer& buf) {
                                                 if (buf.getWatermark() < newWatermark) {
                                                     vec.push_back(buf);
                                                     return true;
                                                 }
                                                 return false;
                                             });
                    bufferVec.erase(it, bufferVec.end());
                }
                NES_DEBUG("returning")
//                for (auto buff: vec) {
//                    writeDataToFile(buff);
//                }
                 writeDataToTCP(vec, sinkInfo);
//            }
        }
        NES_DEBUG("notify checkpoint created");
    }
}

bool FileSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef context) {
    NES_ERROR("Size of buffer storage {}", buffersStorage.size());
    if (!timestampAndWriteToSocket) {
        if (!isOpen) {
            NES_DEBUG("The output file could not be opened during setup of the file sink.");
            return false;
        }
        std::unique_lock lock(writeMutex);

        if (!inputBuffer) {
            NES_ERROR("Invalid input buffer");
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
        NES_DEBUG("Writing tuples to file sink; filePath={}, fBuffer={}", filePath, fBuffer);
        outputFile.write(fBuffer.c_str(), fBuffer.size());
        outputFile.flush();
        return true;
    } else {
        (void) context;
         auto sinkInfo = nodeEngine->getTcpDescriptor(filePath);

        // NES_ERROR("got buffer {}.{}", inputBuffer.getSequenceNumber(), inputBuffer.getChunkNumber());
        if (!getReplayData()) {
            NES_DEBUG("replay data not activated writing buffer");
            std::vector<Runtime::TupleBuffer> vec = {inputBuffer};
//            return writeDataToFile(inputBuffer);
             return writeDataToTCP(vec, sinkInfo);
        }
        numberOfReceivedBuffers++;

        if (!inputBuffer) {
            NES_ERROR("Invalid input buffer");
            return false;
        }

        NES_ASSERT(inputBuffer.getNumberOfTuples() != 0, "number of tuples must not be 0");
        const auto bufferSeqNumber = inputBuffer.getSequenceNumber();
        const auto bufferChunkNumber = inputBuffer.getChunkNumber();
        const auto bufferLastChunk = inputBuffer.isLastChunk();
        const auto bufferWatermark = inputBuffer.getWatermark();
        const auto bufferOriginId = inputBuffer.getOriginId();

        // auto watermarksProcessor = nodeEngine->getWatermarkProcessorFor(filePath, numberOfOrigins);
        // auto bufferStorage = nodeEngine->writeLockSinkStorage(filePath);
        buffersStorage[bufferOriginId.getRawValue()].push_back(inputBuffer);

        auto currentWatermarkBeforeAdding = watermarksProcessor->getCurrentWatermark();

        //ignore this buffer if it was already written
//        if (bufferWatermark < currentWatermarkBeforeAdding) {
//            NES_DEBUG("seq {} is less than {} for id {}", bufferSeqNumber, currentWatermarkBeforeAdding, bufferOriginId);
//            return true;
//        }
//
//        {
//            if (bufferStorage->operator[](bufferOriginId).contains(bufferSeqNumber)) {
//                NES_DEBUG("buffer already recorded, exiting");
//                return true;
//            }
//            NES_DEBUG("buffer not yet recored, proceeedign");
//            bufferStorage->operator[](bufferOriginId).emplace(bufferSeqNumber);
//        }
//        NES_DEBUG("increase seq nr")

        // save the highest consecutive sequence number in the queue

        // create sequence data without chunks, so chunk number is 1 and last chunk flag is true
        const auto seqData = SequenceData(bufferSeqNumber, bufferChunkNumber, bufferLastChunk);
        // insert input buffer sequence number to the queue
        watermarksProcessor->updateWatermark(bufferWatermark, seqData, bufferOriginId);

        // get the highest consecutive sequence number in the queue after adding new value
        auto currentWatermarkAfterAdding = watermarksProcessor->getCurrentWatermark();
        NES_DEBUG("saved watermark: {}", currentWatermarkAfterAdding);

        NES_DEBUG("watermark before adding {}, after adding {}, source id {}, sharedQueryId {}",
                  currentWatermarkBeforeAdding,
                  currentWatermarkAfterAdding,
                  bufferOriginId,
                  sharedQueryId);
        std::vector<Runtime::TupleBuffer> vec;
        NES_DEBUG("writing and erasing elements");
        for (auto& [id, bufferVec] : buffersStorage) {
            auto it = std::remove_if(bufferVec.begin(), bufferVec.end(),
                                     [&](const Runtime::TupleBuffer& buf) {
                                         if (buf.getWatermark() < currentWatermarkAfterAdding) {
                                             vec.push_back(buf);
                                             return true;
                                         }
                                         return false;
                                     });
            bufferVec.erase(it, bufferVec.end());
        }
        NES_DEBUG("returning")
        return writeDataToTCP(vec, sinkInfo);
//        for (auto buff: vec) {
//            writeDataToFile(buff);
//        }
//        return true;
    }
}

bool FileSink::writeDataToTCP(std::vector<Runtime::TupleBuffer>& buffersToWrite,
                              folly::Synchronized<Runtime::TCPSinkInfo>::LockedPtr& sinkInfo) {

    if (timestampAndWriteToSocket) {
        NES_DEBUG("write data to sink with descriptor {} for {}", sinkInfo->sockfd, filePath)

        for (auto bufferToWrite : buffersToWrite) {

            auto* records = bufferToWrite.getBuffer<Record>();
            for (uint64_t i = 0; i < bufferToWrite.getNumberOfTuples(); ++i) {
                records[i].outputTimestamp_1 = getTimestamp();
                records[i].outputTimestamp_2 = getTimestamp();
                // log the record to error
                NES_DEBUG("writing record win start {}, win end {} Left: id  {}, join id  {}, value {}, event timestamp {}, processing timestamp {}, output timestamp {}; Right: id  {}, join id  {}, value {}, event timestamp {}, processing timestamp {}, output timestamp {}, buffer watermark: {}", records[i].winStart, records[i].winEnd, records[i].id_1, records[i].joinId_1, records[i].value_1, records[i].ingestionTimestamp_1, records[i].processingTimestamp_1, records[i].outputTimestamp_1, records[i].id_2, records[i].joinId_2, records[i].value_2, records[i].ingestionTimestamp_2, records[i].processingTimestamp_2, records[i].outputTimestamp_2, bufferToWrite.getWatermark());
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
