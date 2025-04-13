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
#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Network/NetworkSink.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TimeMeasurement.hpp>
#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <utility>
#include <vector>

namespace NES {

CSVSource::CSVSource(SchemaPtr schema,
                     Runtime::BufferManagerPtr bufferManager,
                     Runtime::QueryManagerPtr queryManager,
                     CSVSourceTypePtr csvSourceType,
                     OperatorId operatorId,
                     OriginId originId,
                     StatisticId statisticId,
                     size_t numSourceLocalBuffers,
                     GatheringMode gatheringMode,
                     const std::string& physicalSourceName,
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors,
                     bool addTimestampsAndReadOnStartup)
    : DataSource(schema,
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 originId,
                 statisticId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 physicalSourceName,
                 false,
                 std::move(successors)),
      fileEnded(false), csvSourceType(csvSourceType), filePath(csvSourceType->getFilePath()->getValue()),
      numberOfTuplesToProducePerBuffer(csvSourceType->getNumberOfTuplesToProducePerBuffer()->getValue()),
      delimiter(csvSourceType->getDelimiter()->getValue()), skipHeader(csvSourceType->getSkipHeader()->getValue()),
      addTimeStampsAndReadOnStartup(addTimestampsAndReadOnStartup), shouldGetLastAck(addTimestampsAndReadOnStartup) {

    this->numberOfBuffersToProduce = csvSourceType->getNumberOfBuffersToProduce()->getValue();
    this->gatheringInterval = std::chrono::milliseconds(csvSourceType->getGatheringInterval()->getValue());
    this->tupleSize = schema->getSchemaSizeInBytes();

    NES_DEBUG("Starting tcp source")
    if (numberOfTuplesToProducePerBuffer == 0 && addTimestampsAndReadOnStartup) {
        // NES_DEBUG("Creating source info")
        // auto sourceInfo = this->queryManager->getTcpSourceInfo(physicalSourceName, filePath);
        // NES_DEBUG("Created source info with fd {}", sourceInfo->sockfd)
        return;
    }
    struct Deleter {
        void operator()(const char* ptr) { std::free(const_cast<char*>(ptr)); }
    };
    auto path = std::unique_ptr<const char, Deleter>(const_cast<const char*>(realpath(filePath.c_str(), nullptr)));
    if (path == nullptr) {
        NES_THROW_RUNTIME_ERROR("Could not determine absolute pathname: " << filePath.c_str());
    }

    input.open(path.get());
    if (!(input.is_open() && input.good())) {
        throw Exceptions::RuntimeException("Cannot open file: " + std::string(path.get()));
    }

    NES_DEBUG("CSVSource: Opening path {}", path.get());
    input.seekg(0, std::ifstream::end);
    if (auto const reportedFileSize = input.tellg(); reportedFileSize == -1) {
        throw Exceptions::RuntimeException("CSVSource::CSVSource File " + filePath + " is corrupted");
    } else {
        this->fileSize = static_cast<decltype(this->fileSize)>(reportedFileSize);
    }

    NES_DEBUG("CSVSource: tupleSize={} freq={}ms numBuff={} numberOfTuplesToProducePerBuffer={}",
              this->tupleSize,
              this->gatheringInterval.count(),
              this->numberOfBuffersToProduce,
              this->numberOfTuplesToProducePerBuffer);

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }

    //todo: we are not usign the parser when timestamping
    //this->inputParser = std::make_shared<CSVParser>(schema->getSize(), physicalTypes, delimiter, addTimeStampsAndReadOnStartup);
    this->inputParser = std::make_shared<CSVParser>(schema->getSize(), physicalTypes, delimiter);
    //    if (addTimeStampsAndReadOnStartup) {
    //        NES_TRACE("CSVSource::fillBuffer: start at pos={} fileSize={}", currentPositionInFile, fileSize);
    //        if (this->fileEnded) {
    //            NES_WARNING("CSVSource::fillBuffer: but file has already ended");
    //            return;
    //        }
    //
    //        input.seekg(currentPositionInFile, std::ifstream::beg);
    //
    //        std::string line;
    //        uint64_t tupleCount = 0;
    //
    //        if (skipHeader && currentPositionInFile == 0) {
    //            NES_TRACE("CSVSource: Skipping header");
    //            std::getline(input, line);
    //            currentPositionInFile = input.tellg();
    //        }
    //        while (true) {
    //            //Check if EOF has reached
    //            if (auto const tg = input.tellg(); (tg >= 0 && static_cast<uint64_t>(tg) >= fileSize) || tg == -1) {
    //                NES_TRACE("CSVSource::fillBuffer: reset tellg()={} fileSize={}", input.tellg(), fileSize);
    //                input.clear();
    //                NES_TRACE("CSVSource::fillBuffer: break because file ended");
    //                this->fileEnded = true;
    //                break;
    //            }
    //
    //            std::getline(input, line);
    //            NES_NOT_IMPLEMENTED();
    //            //            readLines.push_back(line);
    //            NES_TRACE("CSVSource line={} val={}", tupleCount, line);
    //        }
    //    }
}

struct Record {
    uint64_t id;
    uint64_t joinId;
    uint64_t value;
    uint64_t ingestionTimestamp;
    uint64_t processingTimestamp;
    uint64_t outputTimestamp;
};

std::optional<Runtime::TupleBuffer>
CSVSource::fillReplayBuffer(folly::Synchronized<Runtime::TcpSourceInfo>::LockedPtr& sourceInfo,
                            Runtime::MemoryLayouts::TestTupleBuffer& buffer) {
    //    auto replay = &sourceInfo->records[replayOffset];
    const auto& innerVec = sourceInfo->records.at(watermarkIndex.first);
    std::vector<Runtime::Record> replayVec(innerVec.begin() + watermarkIndex.second, innerVec.end());
    //    auto replay = sourceInfo->records.at(replayOffset).data();
    auto* records = buffer.getBuffer().getBuffer<Record>();
    buffer.setNumberOfTuples(replayVec.size());

    for (uint64_t i = 0; i < replayVec.size(); ++i) {
        records[i].id = replayVec[i].id;
        records[i].joinId = replayVec[i].joinId;
        records[i].value = replayVec[i].value;
        records[i].ingestionTimestamp = replayVec[i].ingestionTimestamp;
        //                    records[i].processingTimestamp = getTimestamp();
        records[i].processingTimestamp = replayVec[i].processingTimestamp;
        NES_DEBUG("record: id {}, joinId {}, value: {}", records[i].id, records[i].joinId, records[i].value);
    }
    NES_DEBUG("setting sequence number replayed buffer to {} (id {}), fd {}",
              sentUntil,
              sourceInfo->records.front().front().id,
              sourceInfo->sockfd)
    //    buffer.getBuffer().setSequenceNumber(watermarkIndex.first);
    watermarkIndex.first++;
    watermarkIndex.second = 0;
    NES_DEBUG("replay with watermark {} index {}, end of replay at {}",
              replayVec.front().value,
              watermarkIndex.first,
              sourceInfo->records.size());

    return buffer.getBuffer();
}

std::pair<size_t, size_t> CSVSource::findWatermarkIndex(const std::vector<std::vector<Runtime::Record>>& records,
                                                        uint64_t sentUntil) {
    for (size_t outerIndex = 0; outerIndex < records.size(); ++outerIndex) {
        const auto& innerVec = records[outerIndex];
        for (size_t innerIndex = 0; innerIndex < innerVec.size(); ++innerIndex) {
            if (innerVec[innerIndex].value >= sentUntil) {
                // Found, return indices as watermarkIndex
                return {outerIndex, innerIndex};
            }
        }
    }
    // Not found case, can return invalid index (optional handling)
    return {static_cast<size_t>(-1), static_cast<size_t>(-1)};
}

std::optional<Runtime::TupleBuffer> CSVSource::receiveData() {
    NES_DEBUG("CSVSource::receiveData called on  {}", operatorId);
    auto buffer = allocateBuffer();
    if (addTimeStampsAndReadOnStartup) {
        auto sourceInfo = queryManager->getTcpSourceInfo(physicalSourceName, filePath);
        if (getReplayData()) {
            if (sourceInfo->records.empty()) {
                // not to wait acknowledgment on the first start of source
                shouldGetLastAck = false;
            }
            //            NES_ERROR("there are {} stored buffers", sourceInfo->records.size());
            if (!sourceInfo->records.empty() && shouldGetLastAck) {
                //                NES_ERROR("tuples were read before from this descriptor, waiting for ack");
                auto decomposedQueryPlans = queryManager->getExecutablePlanIdsForSource(shared_from_base<DataSource>());
                auto sharedQueryPlan = queryManager->getSharedQueryId(*decomposedQueryPlans.begin());
                auto ack = queryManager->getSourceAck(sharedQueryPlan.getRawValue());
                shouldGetLastAck = false;
                if (ack.has_value() && ack.value() != 0) {
                    // NES_ERROR("{} found ack, sent until {} ack {}", sharedQueryPlan.getRawValue(), sentUntil, ack.value());
                    watermarkIndex = findWatermarkIndex(sourceInfo->records, ack.value());
                    // NES_ERROR("id {}, index: {}, of: {}, watermark: {}", sharedQueryPlan.getRawValue(), watermarkIndex.first, sourceInfo->records.size(), sourceInfo->records.at(watermarkIndex.first)[watermarkIndex.second].value);
                }
            }
            //            if (watermarkIndex < sourceInfo->records.back().back().ingestionTimestamp) {
            if (watermarkIndex.first < sourceInfo->records.size()) {
                NES_DEBUG("sent untill {}, records {}", sentUntil, sourceInfo->records.size());
                return fillReplayBuffer(sourceInfo, buffer);
            }
        }
        if (!sourceInfo->records.empty()) {
            NES_DEBUG("records {}, sent until {}", sourceInfo->records.size(), sentUntil);
        }
        NES_ASSERT(!getReplayData() || watermarkIndex.first <= sourceInfo->records.size(),
                   "sent until cannot be more than recorded buffers");
        uint64_t generatedTuplesThisPass = 0;
        generatedTuplesThisPass = buffer.getCapacity();
        auto* records = buffer.getBuffer().getBuffer<Record>();
        //if port was set, use tcp as input
        if (numberOfTuplesToProducePerBuffer == 0) {
            //            auto testUsingPort = false;
            //            if (testUsingPort) {
            if (sourceInfo->port != 0) {
                //init flush interval value
                bool flushIntervalPassed = false;
                auto flushIntervalTimerStart = std::chrono::system_clock::now();

                //init tuple count for buffer
                uint64_t tupleCount = 0;
                //todo: do not hardcode
                auto valueSize = sizeof(uint64_t);
                auto incomingTupleSize = valueSize * 4;
                //todo: move to beginning
                auto bytesPerBuffer = generatedTuplesThisPass * incomingTupleSize;
                sourceInfo->incomingBuffer.reserve(bytesPerBuffer);
                sourceInfo->leftOverBytes.reserve(incomingTupleSize);
                for (uint16_t i = 0; i < sourceInfo->leftoverByteCount; ++i) {
                    sourceInfo->incomingBuffer[i] = sourceInfo->leftOverBytes[i];
                }
                auto byteOffset = sourceInfo->leftoverByteCount;
                //todo: this was new
                while (byteOffset < bytesPerBuffer) {
                    NES_DEBUG("TCPSource::fillBuffer: reading from socket");
                    int bytesRead =
                        read(sourceInfo->sockfd, &sourceInfo->incomingBuffer[byteOffset], bytesPerBuffer - byteOffset);
                    NES_DEBUG("TCPSource::fillBuffer: {} bytes read", bytesRead);
                    if (bytesRead == 0) {
                        if (byteOffset < incomingTupleSize) {
                            //NES_ERROR("Read timed out before complete tuple was read");
                            //return std::nullopt;
                            //                                buffer.setNumberOfTuples(0);
                            //                                return buffer.getBuffer();
                            if (running) {
                                break;
                            } else {
                                return std::nullopt;
                            }
                            // buffer.setNumberOfTuples(0);
                            // return buffer.getBuffer();
                        }
                        //flushIntervalPassed = true;
                    } else if (bytesRead < 0) {
                        // NES_ERROR("TCPSource::fillBuffer: error while reading from socket");
                        if (running) {
                            break;
                        } else {
                            return std::nullopt;
                        }
                    }
                    byteOffset += bytesRead;
                }
                uint64_t numCompleteTuplesRead = byteOffset / incomingTupleSize;


                NES_DEBUG("checking if replay is activated")
                if (getReplayData()) {
                    NES_DEBUG("replay activated creating new stored buffer")
                    sourceInfo->records.push_back({});
                }
                for (uint64_t i = 0; i < numCompleteTuplesRead; ++i) {
                    auto index = i * incomingTupleSize;
                    auto id = reinterpret_cast<uint64_t*>(&(sourceInfo->incomingBuffer[index]));
                    auto joinId = reinterpret_cast<uint64_t*>(&(sourceInfo->incomingBuffer[index + valueSize]));
                    auto seqenceNr = reinterpret_cast<uint64_t*>(&(sourceInfo->incomingBuffer[index + 2 * valueSize]));
                    auto ingestionTime = reinterpret_cast<uint64_t*>(&(sourceInfo->incomingBuffer[index + 3 * valueSize]));
                    //std::cout << "id: " << *id << " seq: " << *seqenceNr << " time: " << *ingestionTime << std::endl;
                    auto timeStamp = getTimestamp();
                    records[i].id = *id;
                    records[i].joinId = *joinId;
                    records[i].value = *seqenceNr;
                    records[i].ingestionTimestamp = *ingestionTime;
                    //                    records[i].processingTimestamp = getTimestamp();
                    records[i].processingTimestamp = timeStamp;

                    if (getReplayData()) {
                        //                    sourceInfo->records.emplace_back(*id, *seqenceNr, *ingestionTime, timeStamp);
                        sourceInfo->records.back()
                            .emplace_back(*id, *joinId, *seqenceNr, *ingestionTime, timeStamp, *ingestionTime);
                        //totalTupleCount++;
                    }
                }
                auto bytesOfCompleteTuples = incomingTupleSize * numCompleteTuplesRead;
                //todo: this is only for debugging
                //auto oldLeftoverBytes = leftoverByteCount;
                sourceInfo->leftoverByteCount = byteOffset % incomingTupleSize;
                for (uint64_t i = 0; i < sourceInfo->leftoverByteCount; ++i) {
                    sourceInfo->leftOverBytes[i] = sourceInfo->incomingBuffer[i + bytesOfCompleteTuples];
                }

                if (numCompleteTuplesRead == 0) {
                    //NES_ERROR("No complete tuples read, returning empty buffer");
                    buffer.setNumberOfTuples(numCompleteTuplesRead);
                    return  buffer.getBuffer();
                }

                buffer.setNumberOfTuples(numCompleteTuplesRead);
                // NES_ERROR("number of tuples: {}", numCompleteTuplesRead);
                generatedTuples += numCompleteTuplesRead;
                generatedBuffers++;
                auto returnBuffer = buffer.getBuffer();
                if (getReplayData()) {
                    //sequence numbers start at 1 which is why we can use size
                    NES_DEBUG("setting sequence number first played buffer to {} (id {})",
                              sourceInfo->records.size(),
                              sourceInfo->records.front().front().id)
                    //                        returnBuffer.setSequenceNumber(sourceInfo->records.size());
                }
                // TODO: check this logic precisely
                sentUntil = sourceInfo->records.back().back().value;
                watermarkIndex.first++;
                return returnBuffer;
            } else {
                uint64_t valCount = 0;
                for (auto u = 0u; u < generatedTuplesThisPass; ++u) {
                    records[u].id = operatorId.getRawValue();
                    records[u].value = valCount + u;
                    records[u].ingestionTimestamp = getTimestamp();
                    records[u].processingTimestamp = 0;
                }
                buffer.setNumberOfTuples(generatedTuplesThisPass);
            }
            generatedTuples += generatedTuplesThisPass;
            generatedBuffers++;
            return buffer.getBuffer();
            //            } else {
            //                fillBufferFromFile(buffer, sourceInfo);
            //                return buffer.getBuffer();
            //            }
        } else {
            generatedTuplesThisPass = numberOfTuplesToProducePerBuffer;
            NES_ASSERT2_FMT(generatedTuplesThisPass * tupleSize < buffer.getBuffer().getBufferSize(), "Wrong parameters");
        }
        NES_TRACE("CSVSource::fillBuffer: fill buffer with #tuples={} of size={}", generatedTuplesThisPass, tupleSize);

        uint64_t tupleCount = 0;
        while (tupleCount < generatedTuplesThisPass) {
            if (sourceInfo->nextLinesIndex == sourceInfo->readLines.size()) {
                break;
            }
            auto line = sourceInfo->readLines[sourceInfo->nextLinesIndex];
            inputParser->writeInputTupleToTupleBuffer(line, tupleCount, buffer, schema, localBufferManager);
            sourceInfo->nextLinesIndex++;
            ++tupleCount;
        }
        buffer.setNumberOfTuples(tupleCount);
        generatedTuples += tupleCount;
        generatedBuffers++;
    } else {
        fillBuffer(buffer);
    }
    NES_TRACE("CSVSource::receiveData filled buffer with tuples= {}", buffer.getNumberOfTuples());

    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer.getBuffer();
}

std::string CSVSource::toString() const {
    std::stringstream ss;
    ss << "CSV_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq=" << this->gatheringInterval.count()
       << "ms"
       << " numBuff=" << this->numberOfBuffersToProduce << ")";
    return ss.str();
}

void CSVSource::fillBufferFromFile(Runtime::MemoryLayouts::TestTupleBuffer& buffer,
                                   folly::Synchronized<Runtime::TcpSourceInfo>::LockedPtr& sourceInfo) {
    NES_TRACE("CSVSource::fillBuffer: start at pos={} fileSize={}", currentPositionInFile, fileSize);
    if (this->fileEnded) {
        NES_WARNING("CSVSource::fillBuffer: but file has already ended");
        return;
    }

    input.seekg(currentPositionInFile, std::ifstream::beg);

    uint64_t generatedTuplesThisPass = 0;
    //fill buffer maximally
    if (numberOfTuplesToProducePerBuffer == 0) {
        generatedTuplesThisPass = buffer.getCapacity();
    } else {
        generatedTuplesThisPass = numberOfTuplesToProducePerBuffer;
        NES_ASSERT2_FMT(generatedTuplesThisPass * tupleSize < buffer.getBuffer().getBufferSize(), "Wrong parameters");
    }
    NES_TRACE("CSVSource::fillBuffer: fill buffer with #tuples={} of size={}", generatedTuplesThisPass, tupleSize);

    std::string line;
    uint64_t tupleCount = 0;

    if (skipHeader && currentPositionInFile == 0) {
        NES_TRACE("CSVSource: Skipping header");
        std::getline(input, line);
        currentPositionInFile = input.tellg();
    }

    while (tupleCount < generatedTuplesThisPass) {

        //Check if EOF has reached
        if (auto const tg = input.tellg(); (tg >= 0 && static_cast<uint64_t>(tg) >= fileSize) || tg == -1) {
            NES_TRACE("CSVSource::fillBuffer: reset tellg()={} fileSize={}", input.tellg(), fileSize);
            input.clear();
            NES_TRACE("CSVSource::fillBuffer: break because file ended");
            this->fileEnded = true;
            break;
        }

        std::getline(input, line);
        NES_TRACE("CSVSource line={} val={}", tupleCount, line);
        // TODO: there will be a problem with non-printable characters (at least with null terminators). Check sources

        inputParser->writeInputTupleToTupleBuffer(line, tupleCount, buffer, schema, localBufferManager);
        tupleCount++;
    }//end of while

    currentPositionInFile = input.tellg();
    buffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;

    if (getReplayData()) {
        auto* records = buffer.getBuffer().getBuffer<Runtime::Record>();
        std::vector<Runtime::Record> newRecords;
        for (uint64_t i = 0; i < tupleCount; i++) {
            newRecords.push_back(records[i]);
        }

        sourceInfo->records.push_back(newRecords);
        watermarkIndex.first++;
    }

    NES_TRACE("CSVSource::fillBuffer: reading finished read {} tuples at posInFile={}", tupleCount, currentPositionInFile);
    NES_TRACE("CSVSource::fillBuffer: read produced buffer=  {}", Util::printTupleBufferAsCSV(buffer.getBuffer(), schema));
}

void CSVSource::fillBuffer(Runtime::MemoryLayouts::TestTupleBuffer& buffer) {
    NES_TRACE("CSVSource::fillBuffer: start at pos={} fileSize={}", currentPositionInFile, fileSize);
    if (this->fileEnded) {
        NES_WARNING("CSVSource::fillBuffer: but file has already ended");
        return;
    }

    input.seekg(currentPositionInFile, std::ifstream::beg);

    uint64_t generatedTuplesThisPass = 0;
    //fill buffer maximally
    if (numberOfTuplesToProducePerBuffer == 0) {
        generatedTuplesThisPass = buffer.getCapacity();
    } else {
        generatedTuplesThisPass = numberOfTuplesToProducePerBuffer;
        NES_ASSERT2_FMT(generatedTuplesThisPass * tupleSize < buffer.getBuffer().getBufferSize(), "Wrong parameters");
    }
    NES_TRACE("CSVSource::fillBuffer: fill buffer with #tuples={} of size={}", generatedTuplesThisPass, tupleSize);

    std::string line;
    uint64_t tupleCount = 0;

    if (skipHeader && currentPositionInFile == 0) {
        NES_TRACE("CSVSource: Skipping header");
        std::getline(input, line);
        currentPositionInFile = input.tellg();
    }

    while (tupleCount < generatedTuplesThisPass) {

        //Check if EOF has reached
        if (auto const tg = input.tellg(); (tg >= 0 && static_cast<uint64_t>(tg) >= fileSize) || tg == -1) {
            NES_TRACE("CSVSource::fillBuffer: reset tellg()={} fileSize={}", input.tellg(), fileSize);
            input.clear();
            NES_TRACE("CSVSource::fillBuffer: break because file ended");
            this->fileEnded = true;
            break;
        }

        std::getline(input, line);
        NES_TRACE("CSVSource line={} val={}", tupleCount, line);
        // TODO: there will be a problem with non-printable characters (at least with null terminators). Check sources

        inputParser->writeInputTupleToTupleBuffer(line, tupleCount, buffer, schema, localBufferManager);
        tupleCount++;
    }//end of while

    currentPositionInFile = input.tellg();
    buffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    NES_TRACE("CSVSource::fillBuffer: reading finished read {} tuples at posInFile={}", tupleCount, currentPositionInFile);
    NES_TRACE("CSVSource::fillBuffer: read produced buffer=  {}", Util::printTupleBufferAsCSV(buffer.getBuffer(), schema));
}

SourceType CSVSource::getType() const { return SourceType::CSV_SOURCE; }

std::string CSVSource::getFilePath() const { return filePath; }

const CSVSourceTypePtr& CSVSource::getSourceConfig() const { return csvSourceType; }

CSVSource::~CSVSource() {
    //    auto count = std::dynamic_pointer_cast<Network::NetworkSink>(std::get<DataSinkPtr>(getExecutableSuccessors().front()))
    //                     ->getReconnectCount();
    //    NES_DEBUG("destroying source with reconnect count {}", count);
    //    ::close(sockfd);
    auto sourceInfo = queryManager->getTcpSourceInfo(physicalSourceName, filePath);
    // NES_ERROR("saved records: {}, watermark: {}", sourceInfo->records.size(), sourceInfo->records.back().back().value);
}
}// namespace NES
