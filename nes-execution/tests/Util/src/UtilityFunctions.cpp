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

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Synopses/CountMinStatistic.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Synopses/HyperLogLogStatistic.hpp>
#include <StatisticCollection/StatisticStorage/AbstractStatisticStore.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Execution/Operators/Streaming/SliceAssigner.hpp>
#include <Util/StdInt.hpp>
#include <Util/Common.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <filesystem>
#include <fstream>
#include <random>
#include <set>

namespace NES::Runtime::Execution::Util {

std::vector<TupleBuffer> createDataForOneFieldAndTimeStamp(int numberOfTuples, BufferManager& bufferManager,
                                                           SchemaPtr schema,
                                                           const std::string& fieldToBuildCountMinOver,
                                                           const std::string& timestampFieldName) {
    auto buffer = bufferManager.getBufferBlocking();
    std::vector<TupleBuffer> inputBuffers;

    SequenceNumber sequenceNumber = INVALID_SEQ_NUMBER;
    StatisticId statisticId = 1;
    ChunkNumber chunkNumber = 1; // As we do not split a sequence number over multiple buffers here
    OriginId originId = 0; // As we only have one origin in all tests

    for (auto i = 0; i < numberOfTuples; ++i) {
        auto dynamicBuffer = MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);
        auto curTuplePos = dynamicBuffer.getNumberOfTuples();
        dynamicBuffer[curTuplePos][fieldToBuildCountMinOver].write<uint64_t>(rand());
        dynamicBuffer[curTuplePos][timestampFieldName].write<uint64_t>(i);
        dynamicBuffer.setNumberOfTuples(curTuplePos + 1);

        if (dynamicBuffer.getNumberOfTuples() >= dynamicBuffer.getCapacity()) {
            buffer.setStatisticId(statisticId);
            buffer.setSequenceData({++sequenceNumber, chunkNumber, true});
            buffer.setOriginId(originId);
            inputBuffers.emplace_back(buffer);
            buffer = bufferManager.getBufferBlocking();
        }
    }

    if (buffer.getNumberOfTuples() > 0) {
        buffer.setStatisticId(statisticId);
        buffer.setSequenceData({++sequenceNumber, chunkNumber, true});
        buffer.setOriginId(originId);
        inputBuffers.emplace_back(buffer);
    }

    return inputBuffers;
}

void updateTestCountMinStatistic(MemoryLayouts::TestTupleBuffer& testTupleBuffer, Statistic::AbstractStatisticStorePtr statisticStore,
                                            Statistic::MetricHash metricHash, uint64_t numberOfBitsInKey,
                                            uint64_t windowSize, uint64_t windowSlide, uint64_t width, uint64_t depth,
                                 const std::string& fieldToBuildCountMinOver,
                                 const std::string& timestampFieldName) {

    // 1. Creating h3Seeds nad H3Hash function
    constexpr auto numberOfBitsInHashValue = NUMBER_OF_BITS_IN_HASH_VALUE;
    std::vector<uint64_t> h3Seeds;
    std::random_device rd;
    std::mt19937 gen(H3_SEED);
    std::uniform_int_distribution<uint64_t> distribution;
    for (auto row = 0UL; row < depth; ++row) {
        for (auto keyBit = 0UL; keyBit < numberOfBitsInKey; ++keyBit) {
            h3Seeds.emplace_back(distribution(gen));
        }
    }
    std::unique_ptr<Nautilus::Interface::HashFunction> h3Hash = std::make_unique<Nautilus::Interface::H3Hash>(numberOfBitsInKey);

    // 2. For each tuple in the buffer, we get the corresponding count min statistic and then update it accordingly
    auto statisticId = testTupleBuffer.getBuffer().getStatisticId();
    Operators::SliceAssigner sliceAssigner(windowSize, windowSlide);
    for (auto tuple : testTupleBuffer){
        auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
        auto ts = tuple[timestampFieldName].read<uint64_t>();
        auto startTs = Windowing::TimeMeasure(sliceAssigner.getSliceStartTs(ts));
        auto endTs = Windowing::TimeMeasure(sliceAssigner.getSliceEndTs(ts));

        auto allCountMinStatistics = statisticStore->getStatistics(statisticHash, startTs, endTs);
        Statistic::StatisticPtr countMinStatistic;
        if (allCountMinStatistics.empty()) {
            countMinStatistic = Statistic::CountMinStatistic::createInit(startTs, endTs, width, depth, numberOfBitsInKey);
            statisticStore->insertStatistic(statisticHash, countMinStatistic);
            NES_DEBUG("Created and inserted new countMinStatistic = {} for statisticHash = {}", countMinStatistic->toString(), statisticHash);
        } else {
            countMinStatistic = allCountMinStatistics[0];
        }

        for (auto row = 0_u64; row < depth; ++row) {
            int8_t* h3SeedsStart = (int8_t*) h3Seeds.data();
            auto h3SeedsOffSet = row * ((numberOfBitsInKey * numberOfBitsInHashValue) / 8);
            Nautilus::Value<Nautilus::MemRef> h3SeedMemRef(h3SeedsStart + h3SeedsOffSet);
            Nautilus::Value<Nautilus::UInt64> valKey(tuple[fieldToBuildCountMinOver].read<uint64_t>());
            auto calcHash = h3Hash->calculateWithState(valKey, h3SeedMemRef).getValue().getValue();
            auto col = calcHash % width;
            countMinStatistic->as<Statistic::CountMinStatistic>()->update(row, col);
        }
    }
}

void updateTestHyperLogLogStatistic(MemoryLayouts::TestTupleBuffer& testTupleBuffer,
                                    Statistic::AbstractStatisticStorePtr statisticStore,
                                    Statistic::MetricHash metricHash,
                                    uint64_t windowSize,
                                    uint64_t windowSlide,
                                    uint64_t width,
                                    const std::string& fieldToBuildCountMinOver,
                                    const std::string& timestampFieldName) {


    // For each tuple in the buffer, we get the corresponding hyperloglog statistic and then update it accordingly
    std::unique_ptr<Nautilus::Interface::HashFunction> murmurHash = std::make_unique<Nautilus::Interface::MurMur3HashFunction>();
    auto statisticId = testTupleBuffer.getBuffer().getStatisticId();
    Operators::SliceAssigner sliceAssigner(windowSize, windowSlide);
    for (auto tuple : testTupleBuffer){
        auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
        auto ts = tuple[timestampFieldName].read<uint64_t>();
        auto startTs = Windowing::TimeMeasure(sliceAssigner.getSliceStartTs(ts));
        auto endTs = Windowing::TimeMeasure(sliceAssigner.getSliceEndTs(ts));

        auto allHyperLogLogStatistics = statisticStore->getStatistics(statisticHash, startTs, endTs);
        Statistic::StatisticPtr hllStatistic;
        if (allHyperLogLogStatistics.empty()) {
            hllStatistic = Statistic::HyperLogLogStatistic::createInit(startTs, endTs, width);
            statisticStore->insertStatistic(statisticHash, hllStatistic);
            NES_DEBUG("Created and inserted new hllStatistic = {} for statisticHash = {}",
                      hllStatistic->toString(), statisticHash);
        } else {
            hllStatistic = allHyperLogLogStatistics[0];
        }

        Nautilus::Value<Nautilus::UInt64> valKey(tuple[fieldToBuildCountMinOver].read<uint64_t>());
        auto hash = murmurHash->calculate(valKey);
        hllStatistic->as<Statistic::HyperLogLogStatistic>()->update(hash->getValue());
    }
}

Runtime::TupleBuffer getBufferFromPointer(uint8_t* recordPtr, const SchemaPtr& schema, BufferManagerPtr bufferManager) {
    auto buffer = bufferManager->getBufferBlocking();
    uint8_t* bufferPtr = buffer.getBuffer();

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto& field : schema->fields) {
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        std::memcpy(bufferPtr, recordPtr, fieldType->size());
        bufferPtr += fieldType->size();
        recordPtr += fieldType->size();
    }
    buffer.setNumberOfTuples(1);
    return buffer;
}

void writeNautilusRecord(uint64_t recordIndex,
                         int8_t* baseBufferPtr,
                         Nautilus::Record nautilusRecord,
                         SchemaPtr schema,
                         BufferManagerPtr bufferManager) {
    Nautilus::Value<Nautilus::UInt64> nautilusRecordIndex(recordIndex);
    Nautilus::Value<Nautilus::MemRef> nautilusBufferPtr(baseBufferPtr);
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        auto rowMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());
        auto memoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(rowMemoryLayout);

        memoryProviderPtr->write(nautilusRecordIndex, nautilusBufferPtr, nautilusRecord);

    } else if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        auto columnMemoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, bufferManager->getBufferSize());
        auto memoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(columnMemoryLayout);

        memoryProviderPtr->write(nautilusRecordIndex, nautilusBufferPtr, nautilusRecord);

    } else {
        NES_THROW_RUNTIME_ERROR("Schema Layout not supported!");
    }
}

Runtime::TupleBuffer mergeBuffers(std::vector<Runtime::TupleBuffer>& buffersToBeMerged,
                                  const SchemaPtr schema,
                                  Runtime::BufferManagerPtr bufferManager) {

    auto retBuffer = bufferManager->getBufferBlocking();
    auto retBufferPtr = retBuffer.getBuffer();

    auto maxPossibleTuples = retBuffer.getBufferSize() / schema->getSchemaSizeInBytes();
    auto cnt = 0UL;
    for (auto& buffer : buffersToBeMerged) {
        cnt += buffer.getNumberOfTuples();
        if (cnt > maxPossibleTuples) {
            NES_WARNING("Too many tuples to fit in a single buffer.");
            return retBuffer;
        }

        auto bufferSize = buffer.getNumberOfTuples() * schema->getSchemaSizeInBytes();
        std::memcpy(retBufferPtr, buffer.getBuffer(), bufferSize);

        retBufferPtr += bufferSize;
        retBuffer.setNumberOfTuples(cnt);
    }

    return retBuffer;
}

std::vector<Runtime::TupleBuffer> mergeBuffersSameWindow(std::vector<Runtime::TupleBuffer>& buffers,
                                                         SchemaPtr schema,
                                                         const std::string& timeStampFieldName,
                                                         BufferManagerPtr bufferManager,
                                                         uint64_t windowSize) {
    if (buffers.size() == 0) {
        return {};
    }

    if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        NES_FATAL_ERROR("Column layout is not support for this function currently!");
    }

    NES_INFO("Merging buffers together!");

    std::vector<Runtime::TupleBuffer> retVector;

    auto curBuffer = bufferManager->getBufferBlocking();
    auto numberOfTuplesInBuffer = 0UL;
    auto lastTimeStamp = windowSize - 1;
    for (auto buf : buffers) {
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());
        auto testTupleBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buf);

        for (auto curTuple = 0UL; curTuple < testTupleBuffer.getNumberOfTuples(); ++curTuple) {
            if (testTupleBuffer[curTuple][timeStampFieldName].read<uint64_t>() > lastTimeStamp
                || numberOfTuplesInBuffer >= memoryLayout->getCapacity()) {

                if (testTupleBuffer[curTuple][timeStampFieldName].read<uint64_t>() > lastTimeStamp) {
                    lastTimeStamp += windowSize;
                }

                curBuffer.setNumberOfTuples(numberOfTuplesInBuffer);
                retVector.emplace_back(std::move(curBuffer));

                curBuffer = bufferManager->getBufferBlocking();
                numberOfTuplesInBuffer = 0;
            }

            memcpy(curBuffer.getBuffer() + schema->getSchemaSizeInBytes() * numberOfTuplesInBuffer,
                   buf.getBuffer() + schema->getSchemaSizeInBytes() * curTuple,
                   schema->getSchemaSizeInBytes());
            numberOfTuplesInBuffer += 1;
            curBuffer.setNumberOfTuples(numberOfTuplesInBuffer);
        }
    }

    if (numberOfTuplesInBuffer > 0) {
        curBuffer.setNumberOfTuples(numberOfTuplesInBuffer);
        retVector.emplace_back(std::move(curBuffer));
    }

    return retVector;
}

std::vector<Runtime::TupleBuffer> sortBuffersInTupleBuffer(std::vector<Runtime::TupleBuffer>& buffersToSort,
                                                           SchemaPtr schema,
                                                           const std::string& sortFieldName,
                                                           BufferManagerPtr bufferManager) {
    if (buffersToSort.size() == 0) {
        return {};
    }
    if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        NES_FATAL_ERROR("Column layout is not support for this function currently!");
    }

    std::vector<Runtime::TupleBuffer> retVector;
    for (auto bufRead : buffersToSort) {
        std::vector<size_t> indexAlreadyInNewBuffer;
        auto memLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize());
        auto testTupleBuf = Runtime::MemoryLayouts::TestTupleBuffer(memLayout, bufRead);

        auto bufRet = bufferManager->getBufferBlocking();

        for (auto outer = 0UL; outer < bufRead.getNumberOfTuples(); ++outer) {
            auto smallestIndex = bufRead.getNumberOfTuples() + 1;
            for (auto inner = 0UL; inner < bufRead.getNumberOfTuples(); ++inner) {
                if (std::find(indexAlreadyInNewBuffer.begin(), indexAlreadyInNewBuffer.end(), inner)
                    != indexAlreadyInNewBuffer.end()) {
                    // If we have already moved this index into the
                    continue;
                }

                auto sortValueCur = testTupleBuf[inner][sortFieldName].read<uint64_t>();
                auto sortValueOld = testTupleBuf[smallestIndex][sortFieldName].read<uint64_t>();

                if (smallestIndex == bufRead.getNumberOfTuples() + 1) {
                    smallestIndex = inner;
                    continue;
                } else if (sortValueCur < sortValueOld) {
                    smallestIndex = inner;
                }
            }
            indexAlreadyInNewBuffer.emplace_back(smallestIndex);
            auto posRet = bufRet.getNumberOfTuples();
            memcpy(bufRet.getBuffer() + posRet * schema->getSchemaSizeInBytes(),
                   bufRead.getBuffer() + smallestIndex * schema->getSchemaSizeInBytes(),
                   schema->getSchemaSizeInBytes());
            bufRet.setNumberOfTuples(posRet + 1);
        }
        retVector.emplace_back(bufRet);
        bufRet = bufferManager->getBufferBlocking();
    }

    return retVector;
}

Runtime::TupleBuffer
getBufferFromRecord(const Nautilus::Record& nautilusRecord, SchemaPtr schema, BufferManagerPtr bufferManager) {
    auto buffer = bufferManager->getBufferBlocking();
    auto* bufferPtr = (int8_t*) buffer.getBuffer();

    writeNautilusRecord(0, bufferPtr, nautilusRecord, std::move(schema), bufferManager);

    buffer.setNumberOfTuples(1);
    return buffer;
}

std::string printTupleBufferAsCSV(Runtime::TupleBuffer tbuffer, const SchemaPtr& schema) {
    std::stringstream ss;
    auto numberOfTuples = tbuffer.getNumberOfTuples();
    auto* buffer = tbuffer.getBuffer<char>();
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (uint64_t i = 0; i < numberOfTuples; i++) {
        uint64_t offset = 0;
        for (uint64_t j = 0; j < schema->getSize(); j++) {
            auto field = schema->get(j);
            auto ptr = field->getDataType();
            auto physicalType = physicalDataTypeFactory.getPhysicalType(ptr);
            auto fieldSize = physicalType->size();
            auto str = physicalType->convertRawToString(buffer + offset + i * schema->getSchemaSizeInBytes());
            ss << str.c_str();
            if (j < schema->getSize() - 1) {
                ss << ",";
            }
            offset += fieldSize;
        }
        ss << std::endl;
    }
    return ss.str();
}

[[maybe_unused]] std::vector<Runtime::TupleBuffer> createBuffersFromCSVFile(const std::string& csvFile,
                                                                            const SchemaPtr& schema,
                                                                            Runtime::BufferManagerPtr bufferManager,
                                                                            uint64_t originId,
                                                                            const std::string& timestampFieldname) {
    std::vector<Runtime::TupleBuffer> recordBuffers;
    NES_ASSERT2_FMT(std::filesystem::exists(std::filesystem::path(csvFile)), "CSVFile " << csvFile << " does not exist!!!");

    // Creating everything for the csv parser
    std::ifstream file(csvFile);
    std::istream_iterator<std::string> beginIt(file);
    std::istream_iterator<std::string> endIt;
    const std::string delimiter = ",";

    // Do-while loop for checking, if we have another line to parse from the inputFile
    const auto maxTuplesPerBuffer = bufferManager->getBufferSize() / schema->getSchemaSizeInBytes();
    auto it = beginIt;
    auto tupleCount = 0UL;
    auto tupleBuffer = bufferManager->getBufferBlocking();
    const auto numberOfSchemaFields = schema->fields.size();
    const auto physicalTypes = getPhysicalTypes(schema);

    uint64_t sequenceNumber = 0;
    uint64_t watermarkTS = 0;
    do {
        std::string line = *it;
        auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(tupleBuffer, schema);
        auto values = NES::Util::splitWithStringDelimiter<std::string>(line, delimiter);

        // iterate over fields of schema and cast string values to correct type
        for (uint64_t j = 0; j < numberOfSchemaFields; j++) {
            auto field = physicalTypes[j];
            NES_TRACE("Current value is:  {}", values[j]);
            writeFieldValueToTupleBuffer(values[j], j, testBuffer, schema, tupleCount, bufferManager);
        }
        if (schema->contains(timestampFieldname)) {
            watermarkTS = std::max(watermarkTS, testBuffer[tupleCount][timestampFieldname].read<uint64_t>());
        }
        ++tupleCount;

        if (tupleCount >= maxTuplesPerBuffer) {
            tupleBuffer.setNumberOfTuples(tupleCount);
            tupleBuffer.setOriginId(originId);
            tupleBuffer.setSequenceNumber(++sequenceNumber);
            tupleBuffer.setWatermark(watermarkTS);
            NES_DEBUG("watermarkTS {} sequenceNumber {} originId {}", watermarkTS, sequenceNumber, originId);

            recordBuffers.emplace_back(tupleBuffer);
            tupleBuffer = bufferManager->getBufferBlocking();
            tupleCount = 0UL;
            watermarkTS = 0UL;
        }
        ++it;
    } while (it != endIt);

    if (tupleCount > 0) {
        tupleBuffer.setNumberOfTuples(tupleCount);
        tupleBuffer.setOriginId(originId);
        tupleBuffer.setSequenceNumber(++sequenceNumber);
        tupleBuffer.setWatermark(watermarkTS);
        recordBuffers.emplace_back(tupleBuffer);
        NES_DEBUG("watermarkTS {} sequenceNumber {} originId {}", watermarkTS, sequenceNumber, originId);
    }

    return recordBuffers;
}

void writeFieldValueToTupleBuffer(std::string inputString,
                                  uint64_t schemaFieldIndex,
                                  Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer,
                                  const SchemaPtr& schema,
                                  uint64_t tupleCount,
                                  const Runtime::BufferManagerPtr& bufferManager) {
    auto fields = schema->fields;
    auto dataType = fields[schemaFieldIndex]->getDataType();
    auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);

    if (inputString.empty()) {
        throw Exceptions::RuntimeException("Input string for parsing is empty");
    }
    // TODO replace with csv parsing library #3949
    try {
        if (physicalType->isBasicType()) {
            auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
            switch (basicPhysicalType->nativeType) {
                case NES::BasicPhysicalType::NativeType::INT_8: {
                    auto value = static_cast<int8_t>(std::stoi(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<int8_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_16: {
                    auto value = static_cast<int16_t>(std::stol(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<int16_t>(value);

                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_32: {
                    auto value = static_cast<int32_t>(std::stol(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<int32_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_64: {
                    auto value = static_cast<int64_t>(std::stoll(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<int64_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_8: {
                    auto value = static_cast<uint8_t>(std::stoi(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<uint8_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_16: {
                    auto value = static_cast<uint16_t>(std::stoul(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<uint16_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_32: {
                    auto value = static_cast<uint32_t>(std::stoul(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<uint32_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_64: {
                    auto value = static_cast<uint64_t>(std::stoull(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<uint64_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::FLOAT: {
                    Util::findAndReplaceAll(inputString, ",", ".");
                    auto value = static_cast<float>(std::stof(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<float>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::DOUBLE: {
                    auto value = static_cast<double>(std::stod(inputString));
                    tupleBuffer[tupleCount][schemaFieldIndex].write<double>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::CHAR: {
                    //verify that only a single char was transmitted
                    if (inputString.size() > 1) {
                        NES_FATAL_ERROR("SourceFormatIterator::mqttMessageToNESBuffer: Received non char Value for CHAR Field {}",
                                        inputString.c_str());
                        throw std::invalid_argument("Value " + inputString + " is not a char");
                    }
                    char value = inputString.at(0);
                    tupleBuffer[tupleCount][schemaFieldIndex].write<char>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::TEXT: {
                    NES_TRACE("Parser::writeFieldValueToTupleBuffer(): trying to write the variable length input string: {}"
                              "to tuple buffer",
                              inputString);
                    tupleBuffer[tupleCount].writeVarSized(schemaFieldIndex, inputString, bufferManager.get());

                    break;
                }
                case NES::BasicPhysicalType::NativeType::BOOLEAN: {
                    //verify that a valid bool was transmitted (valid{true,false,0,1})
                    bool value = !strcasecmp(inputString.c_str(), "true") || !strcasecmp(inputString.c_str(), "1");
                    if (!value) {
                        if (strcasecmp(inputString.c_str(), "false") && strcasecmp(inputString.c_str(), "0")) {
                            NES_FATAL_ERROR(
                                "Parser::writeFieldValueToTupleBuffer: Received non boolean value for BOOLEAN field: {}",
                                inputString.c_str());
                            throw std::invalid_argument("Value " + inputString + " is not a boolean");
                        }
                    }
                    tupleBuffer[tupleCount][schemaFieldIndex].write<bool>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UNDEFINED:
                    NES_FATAL_ERROR("Parser::writeFieldValueToTupleBuffer: Field Type UNDEFINED");
            }
        } else {// char array(string) case
            // obtain pointer from buffer to fill with content via strcpy
            char* value = tupleBuffer[tupleCount][schemaFieldIndex].read<char*>();
            // remove quotation marks from start and end of value (ASSUMES QUOTATIONMARKS AROUND STRINGS)
            // improve behavior with json library
            strcpy(value, inputString.c_str());
        }
    } catch (const std::exception& e) {
        NES_ERROR("Failed to convert inputString to desired NES data type. Error: {}", e.what());
    }
}

void findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr) {
    // Get the first occurrence
    uint64_t pos = data.find(toSearch);
    // Repeat till end is reached
    while (pos != std::string::npos) {
        // Replace this occurrence of Sub String
        data.replace(pos, toSearch.size(), replaceStr);
        // Get the next occurrence from the current position
        pos = data.find(toSearch, pos + replaceStr.size());
    }
}

std::vector<PhysicalTypePtr> getPhysicalTypes(SchemaPtr schema) {
    std::vector<PhysicalTypePtr> retVector;

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        retVector.push_back(physicalField);
    }

    return retVector;
}

bool checkIfBuffersAreEqual(Runtime::TupleBuffer buffer1, Runtime::TupleBuffer buffer2, uint64_t schemaSizeInByte) {
    NES_DEBUG("Checking if the buffers are equal, so if they contain the same tuples...");
    if (buffer1.getNumberOfTuples() != buffer2.getNumberOfTuples()) {
        NES_DEBUG("Buffers do not contain the same tuples, as they do not have the same number of tuples");
        return false;
    }

    std::set<uint64_t> sameTupleIndices;
    for (auto idxBuffer1 = 0UL; idxBuffer1 < buffer1.getNumberOfTuples(); ++idxBuffer1) {
        bool idxFoundInBuffer2 = false;
        for (auto idxBuffer2 = 0UL; idxBuffer2 < buffer2.getNumberOfTuples(); ++idxBuffer2) {
            if (sameTupleIndices.contains(idxBuffer2)) {
                continue;
            }

            auto startPosBuffer1 = buffer1.getBuffer() + schemaSizeInByte * idxBuffer1;
            auto startPosBuffer2 = buffer2.getBuffer() + schemaSizeInByte * idxBuffer2;
            auto equalTuple = (std::memcmp(startPosBuffer1, startPosBuffer2, schemaSizeInByte) == 0);
            if (equalTuple) {
                sameTupleIndices.insert(idxBuffer2);
                idxFoundInBuffer2 = true;
                break;
            }
        }

        if (!idxFoundInBuffer2) {
            NES_DEBUG("Buffers do not contain the same tuples, as tuple could not be found in both buffers for idx: {}",
                      idxBuffer1);
            return false;
        }
    }

    return (sameTupleIndices.size() == buffer1.getNumberOfTuples());
}
}// namespace NES::Runtime::Execution::Util