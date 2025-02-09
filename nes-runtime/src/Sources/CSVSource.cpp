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
      addTimeStampsAndReadOnStartup(addTimestampsAndReadOnStartup) {

    this->numberOfBuffersToProduce = csvSourceType->getNumberOfBuffersToProduce()->getValue();
    this->gatheringInterval = std::chrono::milliseconds(csvSourceType->getGatheringInterval()->getValue());
    this->tupleSize = schema->getSchemaSizeInBytes();

    NES_DEBUG("Starting tcp source")
    if (numberOfTuplesToProducePerBuffer == 0 && addTimestampsAndReadOnStartup) {
        NES_DEBUG("Creating source info")
        auto sourceInfo = this->queryManager->getTcpSourceInfo(physicalSourceName, filePath);
        sourceInfo->hasCheckedAcknowledgement = false;
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
    if (addTimeStampsAndReadOnStartup) {
        NES_TRACE("CSVSource::fillBuffer: start at pos={} fileSize={}", currentPositionInFile, fileSize);
        if (this->fileEnded) {
            NES_WARNING("CSVSource::fillBuffer: but file has already ended");
            return;
        }

        input.seekg(currentPositionInFile, std::ifstream::beg);

        std::string line;
        uint64_t tupleCount = 0;

        if (skipHeader && currentPositionInFile == 0) {
            NES_TRACE("CSVSource: Skipping header");
            std::getline(input, line);
            currentPositionInFile = input.tellg();
        }
        while (true) {
            //Check if EOF has reached
            if (auto const tg = input.tellg(); (tg >= 0 && static_cast<uint64_t>(tg) >= fileSize) || tg == -1) {
                NES_TRACE("CSVSource::fillBuffer: reset tellg()={} fileSize={}", input.tellg(), fileSize);
                input.clear();
                NES_TRACE("CSVSource::fillBuffer: break because file ended");
                this->fileEnded = true;
                break;
            }

            std::getline(input, line);
            NES_NOT_IMPLEMENTED();
            //            readLines.push_back(line);
            NES_TRACE("CSVSource line={} val={}", tupleCount, line);
        }
    }
}

struct Record {
    uint64_t id;
    uint64_t value;
    uint64_t ingestionTimestamp;
    uint64_t processingTimestamp;
    uint64_t outputTimestamp;
};

void CSVSource::fillReplayBuffer(folly::Synchronized<Runtime::TcpSourceInfo>::LockedPtr& sourceInfo,
                                 Runtime::MemoryLayouts::TestTupleBuffer& buffer) {
    auto replayOffset = sourceInfo->replayedUntil.value();
    auto totalTuplesToReplay = sourceInfo->seqReadFromSocketTotal - replayOffset;
    auto numTuplesToReplay = std::min(totalTuplesToReplay, buffer.getCapacity());

    NES_ERROR("replay {} tuples from {}, end of replay at {} ({} to replay in total)",
              numTuplesToReplay,
              replayOffset,
              sourceInfo->seqReadFromSocketTotal,
              totalTuplesToReplay);

    auto replay = &sourceInfo->records[replayOffset];
    auto* records = buffer.getBuffer().getBuffer<Record>();
    std::memcpy(records, replay, numTuplesToReplay);

    sourceInfo->replayedUntil = numTuplesToReplay + sourceInfo->replayedUntil.value();
    NES_ASSERT(sourceInfo->replayedUntil <= sourceInfo->seqReadFromSocketTotal, "To many tuples replayed");
    if (sourceInfo->replayedUntil == sourceInfo->seqReadFromSocketTotal) {
        sourceInfo->replayedUntil = std::nullopt;
    }
}

std::optional<Runtime::TupleBuffer> CSVSource::receiveData() {
    NES_TRACE("CSVSource::receiveData called on  {}", operatorId);
    //    if (generatedBuffers == 0) {
    //        NES_DEBUG("CSVSource::receiveData called on {} and number of buffers is 0", operatorId);
    //    }
    auto buffer = allocateBuffer();
    if (addTimeStampsAndReadOnStartup) {
        auto sourceInfo = queryManager->getTcpSourceInfo(physicalSourceName, filePath);
        if (!sourceInfo->hasCheckedAcknowledgement) {
            NES_ERROR("has checked acknowledgement is false")
            if (sourceInfo->seqReadFromSocketTotal != 0) {
                NES_ERROR("tuples were read before from this descriptor, waiting for ack");
                auto id = sourceInfo->records.front().id;
                auto ack = queryManager->waitForSourceAck(id);
                NES_ERROR("received ack");
                sourceInfo->replayedUntil = ack;
            }
            sourceInfo->hasCheckedAcknowledgement = true;
        }
        if (sourceInfo->replayedUntil.has_value()) {
            fillReplayBuffer(sourceInfo, buffer);
            return buffer.getBuffer();
        }
        uint64_t generatedTuplesThisPass = 0;
        generatedTuplesThisPass = buffer.getCapacity();
        auto* records = buffer.getBuffer().getBuffer<Record>();
        //if port was set, use tcp as input
        if (numberOfTuplesToProducePerBuffer == 0) {
            if (sourceInfo->port != 0) {
                //init flush interval value
                bool flushIntervalPassed = false;
                auto flushIntervalTimerStart = std::chrono::system_clock::now();

                //init tuple count for buffer
                uint64_t tupleCount = 0;
                //todo: do not hardcode
                auto valueSize = sizeof(uint64_t);
                auto incomingTupleSize = valueSize * 3;
                //todo: move to beginning
                auto bytesPerBuffer = generatedTuplesThisPass * incomingTupleSize;
                sourceInfo->incomingBuffer.reserve(bytesPerBuffer);
                sourceInfo->leftOverBytes.reserve(incomingTupleSize);
                for (uint16_t i = 0; i < sourceInfo->leftoverByteCount; ++i) {
                    sourceInfo->incomingBuffer[i] = sourceInfo->leftOverBytes[i];
                }
                //auto writePostion = &incomingBuffer[leftoverByteCount];
                auto byteOffset = sourceInfo->leftoverByteCount;
                //todo: this was new
                while (byteOffset < incomingTupleSize) {
                    //while (tupleCount < generatedTuplesThisPass) {
                    NES_DEBUG("TCPSource::fillBuffer: filling buffer now");
                    while (byteOffset < bytesPerBuffer) {

                        //NES_ASSERT(generatedTuplesThisPass == bufferManager->getBufferSize(), "Buffersizes do not match");

                        // Read data from the socket
                        //int bytesRead = read(sockfd, incomingBuffer.data(), generatedTuplesThisPass * incomingTupleSize);
                        NES_DEBUG("TCPSource::fillBuffer: reading from socket");
                        int bytesRead =
                            read(sourceInfo->sockfd, &sourceInfo->incomingBuffer[byteOffset], bytesPerBuffer - byteOffset);
                        NES_DEBUG("TCPSource::fillBuffer: {} bytes read", bytesRead);
                        if (bytesRead == 0) {
                            if (byteOffset < incomingTupleSize) {
                                NES_DEBUG("TCPSource::fillBuffer: inserting eos");
                                //return std::nullopt;
                                //                                buffer.setNumberOfTuples(0);
                                //                                return buffer.getBuffer();
                                break;
                            }
                            //flushIntervalPassed = true;
                        } else if (bytesRead < 0) {
                            NES_DEBUG("TCPSource::fillBuffer: error while reading from socket");
                            break;
                        }
                        byteOffset += bytesRead;
                        //todo: this was new
                        //tupleCount = byteOffset / incomingTupleSize;
                        //flush interval
                        if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()
                                                                                  - flushIntervalTimerStart)
                                .count()
                            >= std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::milliseconds(250)).count()) {
                            //.count() >= std::chrono::duration_cast<std::chrono::milliseconds>(gatheringInterval).count()) {
                            NES_DEBUG("TCPSource::fillBuffer: Reached TupleBuffer flush interval. Finishing writing to current "
                                      "TupleBuffer.");
                            flushIntervalPassed = true;
                            if (byteOffset > incomingTupleSize) {
                                break;
                            }
                        }
                    }
                }
                //                if (tupleCount <= 0) {
                //                    return std::nullopt;
                //                }
                uint64_t numCompleteTuplesRead = byteOffset / incomingTupleSize;
                //                uint64_t replayBufferOffset = sourceInfo->seqReadFromSocketTotal;
                //                sourceInfo->records.
                for (uint64_t i = 0; i < numCompleteTuplesRead; ++i) {
                    auto index = i * incomingTupleSize;
                    auto id = reinterpret_cast<uint64_t*>(&(sourceInfo->incomingBuffer[index]));
                    auto seqenceNr = reinterpret_cast<uint64_t*>(&(sourceInfo->incomingBuffer[index + valueSize]));
                    auto ingestionTime = reinterpret_cast<uint64_t*>(&(sourceInfo->incomingBuffer[index + 2 * valueSize]));
                    //std::cout << "id: " << *id << " seq: " << *seqenceNr << " time: " << *ingestionTime << std::endl;
                    auto timeStamp = getTimestamp();
                    records[i].id = *id;
                    records[i].value = *seqenceNr;
                    records[i].ingestionTimestamp = *ingestionTime;
                    //                    records[i].processingTimestamp = getTimestamp();
                    records[i].processingTimestamp = timeStamp;

                    sourceInfo->records.emplace_back(*id, *seqenceNr, *ingestionTime, timeStamp);
                    //totalTupleCount++;
                }
                auto bytesOfCompleteTuples = incomingTupleSize * numCompleteTuplesRead;
                //todo: this is only for debugging
                //auto oldLeftoverBytes = leftoverByteCount;
                sourceInfo->leftoverByteCount = byteOffset % incomingTupleSize;
                for (uint64_t i = 0; i < sourceInfo->leftoverByteCount; ++i) {
                    sourceInfo->leftOverBytes[i] = sourceInfo->incomingBuffer[i + bytesOfCompleteTuples];
                }

                buffer.setNumberOfTuples(numCompleteTuplesRead);
                generatedTuples += numCompleteTuplesRead;
                sourceInfo->seqReadFromSocketTotal += numCompleteTuplesRead - 1;
                generatedBuffers++;
                //                NES_DEBUG("TCPSource::fillBuffer: returning {} tuples, ({} in total) consisting of {} new bytes and {} previous "
                //                          "New leftover bytes count: {} ",
                //                          numCompleteTuplesRead,
                //                          generatedTuples, bytesOfCompleteTuples, oldLeftoverBytes, leftoverByteCount);
                return buffer.getBuffer();

                //NES_ASSERT(bytesRead % tupleSize == 0, "bytes read do not align with tuple size");

                //                uint16_t bytesToJoinWithLeftover = 0;
                //
                //
                //                auto additionalTupleRead = 0;
                //                if (leftoverByteCount != 0) {
                //                    bytesToJoinWithLeftover = incomingTupleSize - leftoverByteCount;
                //                    //NES_INFO("Copying {} bytes to complete leftover tuple", bytesToJoinWithLeftover)
                //                    for (uint16_t i = leftoverByteCount; i < incomingTupleSize; ++i) {
                //                        leftOverBytes[i] = incomingBuffer[i - leftoverByteCount];
                //                    }
                //                    additionalTupleRead = 1;
                //                    auto id = reinterpret_cast<uint64_t*>(&leftOverBytes[0]);
                //                    auto seqenceNr = reinterpret_cast<uint64_t*>(&leftOverBytes[valueSize]);
                //                    auto ingestionTime = reinterpret_cast<uint64_t*>(&leftOverBytes[2 * valueSize]);
                //                    records[0].id = *id;
                //                    records[0].value = *seqenceNr;
                //                    records[0].ingestionTimestamp = *ingestionTime;
                //                    records[0].processingTimestamp = getTimestamp();
                //                    //totalTupleCount++;
                //                    //std::cout << "Leftover: id: " << *id << " seq: " << *seqenceNr << " time: " << *ingestionTime << std::endl;
                //                }
                //
                //                //auto sizeOfCompleteTuplesRead = bytesRead - bytesToJoinWithLeftover;
                //                // Calculate the number of tuples read
                //                auto newTupleBytesRead = bytesRead - bytesToJoinWithLeftover;
                //                uint64_t numCompleteTuplesRead = newTupleBytesRead / incomingTupleSize;
                //                for (uint64_t i = 0; i < numCompleteTuplesRead; ++i) {
                //                    auto index = i * incomingTupleSize + bytesToJoinWithLeftover;
                //                    auto id = reinterpret_cast<uint64_t*>(&incomingBuffer[index]);
                //                    auto seqenceNr = reinterpret_cast<uint64_t*>(&incomingBuffer[index + valueSize]);
                //                    auto ingestionTime = reinterpret_cast<uint64_t*>(&incomingBuffer[index + 2 * valueSize]);
                //                    //std::cout << "id: " << *id << " seq: " << *seqenceNr << " time: " << *ingestionTime << std::endl;
                //                    auto writeIndex = i + additionalTupleRead;
                //                    records[writeIndex].id = *id;
                //                    records[writeIndex].value = *seqenceNr;
                //                    records[writeIndex].ingestionTimestamp = *ingestionTime;
                //                    records[writeIndex].processingTimestamp = getTimestamp();
                //                    //totalTupleCount++;
                //                }
                //
                //                leftoverByteCount = newTupleBytesRead % incomingTupleSize;
                //                auto processedBytes = numCompleteTuplesRead * incomingTupleSize + bytesToJoinWithLeftover;
                ////                if (leftoverByteCount != 0) {
                ////                    NES_INFO("Saving {} bytes of incomplete tuple", leftoverByteCount)
                ////                }
                //                for (uint16_t i = 0 ; i < leftoverByteCount; ++i) {
                //                    leftOverBytes[i] = incomingBuffer[i + processedBytes];
                //                }
                //                buffer.setNumberOfTuples(numCompleteTuplesRead + additionalTupleRead);
                //                //NES_INFO("TotalTupleCount {}", totalTupleCount)

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
    //    ::close(sockfd);
}
}// namespace NES
