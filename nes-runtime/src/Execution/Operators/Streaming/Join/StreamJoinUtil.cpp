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
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

#include <fstream>

namespace NES::Runtime::Execution::Util {

SchemaPtr createJoinSchema(SchemaPtr leftSchema, SchemaPtr rightSchema, const std::string& keyFieldName) {
    NES_ASSERT(leftSchema->getLayoutType() == rightSchema->getLayoutType(),
               "Left and right schema do not have the same layout type");
    NES_ASSERT(leftSchema->contains(keyFieldName) || rightSchema->contains(keyFieldName),
               "KeyFieldName = " << keyFieldName << " is not in either left or right schema");

    auto retSchema = Schema::create(leftSchema->getLayoutType());
    auto newQualifierForSystemField = leftSchema->getSourceNameQualifier() + rightSchema->getSourceNameQualifier();

    retSchema->addField(newQualifierForSystemField + "$start", BasicType::UINT64);
    retSchema->addField(newQualifierForSystemField + "$end", BasicType::UINT64);
    retSchema->addField(newQualifierForSystemField + "$key", BasicType::UINT64);

    for (auto& fields : leftSchema->fields) {
        retSchema->addField(fields->getName(), fields->getDataType());
    }

    for (auto& fields : rightSchema->fields) {
        retSchema->addField(fields->getName(), fields->getDataType());
    }
    NES_DEBUG2("Created joinSchema {} from leftSchema {} and rightSchema {}.",
               retSchema->toString(),
               leftSchema->toString(),
               rightSchema->toString());

    return retSchema;
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
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(tupleBuffer, schema);
        auto values = NES::Util::splitWithStringDelimiter<std::string>(line, delimiter);

        // iterate over fields of schema and cast string values to correct type
        for (uint64_t j = 0; j < numberOfSchemaFields; j++) {
            auto field = physicalTypes[j];
            NES_TRACE2("Current value is:  {}", values[j]);
            writeFieldValueToTupleBuffer(values[j], j, dynamicBuffer, schema, tupleCount, bufferManager);
        }
        if (schema->contains(timestampFieldname)) {
            watermarkTS = std::max(watermarkTS, dynamicBuffer[tupleCount][timestampFieldname].read<uint64_t>());
        }
        ++tupleCount;

        if (tupleCount >= maxTuplesPerBuffer) {
            tupleBuffer.setNumberOfTuples(tupleCount);
            tupleBuffer.setOriginId(originId);
            tupleBuffer.setSequenceNumber(++sequenceNumber);
            tupleBuffer.setWatermark(watermarkTS);
            NES_DEBUG2("watermarkTS {} sequenceNumber {} originId {}", watermarkTS, sequenceNumber, originId);

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
        NES_DEBUG2("watermarkTS {} sequenceNumber {} originId {}", watermarkTS, sequenceNumber, originId);
    }

    return recordBuffers;
}

void writeFieldValueToTupleBuffer(std::string inputString,
                                  uint64_t schemaFieldIndex,
                                  Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer,
                                  const SchemaPtr& schema,
                                  uint64_t tupleCount,
                                  const Runtime::BufferManagerPtr& bufferManager) {
    auto fields = schema->fields;
    auto dataType = fields[schemaFieldIndex]->getDataType();
    auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);

    if (inputString.empty()) {
        throw Exceptions::RuntimeException("Input string for parsing is empty");
    }
    // TODO replace with csv parsing library
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
                        NES_FATAL_ERROR2(
                            "SourceFormatIterator::mqttMessageToNESBuffer: Received non char Value for CHAR Field {}",
                            inputString.c_str());
                        throw std::invalid_argument("Value " + inputString + " is not a char");
                    }
                    char value = inputString.at(0);
                    tupleBuffer[tupleCount][schemaFieldIndex].write<char>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::TEXT: {
                    NES_TRACE2("Parser::writeFieldValueToTupleBuffer(): trying to write the variable length input string: {} to "
                               "tuple buffer",
                               inputString);

                    auto sizeOfInputField = inputString.size();
                    auto totalSize = sizeOfInputField + sizeof(uint32_t);
                    auto childTupleBuffer = allocateVariableLengthField(bufferManager, totalSize);

                    NES_ASSERT(
                        childTupleBuffer.getBufferSize() >= totalSize,
                        "Parser::writeFieldValueToTupleBuffer(): Could not write TEXT field to tuple buffer, there was not "
                        "sufficient space available. Required space: "
                            << totalSize << ", available space: " << childTupleBuffer.getBufferSize());

                    // write out the length and the variable-sized text to the child buffer
                    (*childTupleBuffer.getBuffer<uint32_t>()) = sizeOfInputField;
                    std::memcpy(childTupleBuffer.getBuffer() + sizeof(uint32_t), inputString.c_str(), sizeOfInputField);

                    // attach the child buffer to the parent buffer and write the child buffer index in the
                    // schema field index of the tuple buffer
                    auto childIdx = tupleBuffer.getBuffer().storeChildBuffer(childTupleBuffer);
                    tupleBuffer[tupleCount][schemaFieldIndex].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx);

                    break;
                }
                case NES::BasicPhysicalType::NativeType::BOOLEAN: {
                    //verify that a valid bool was transmitted (valid{true,false,0,1})
                    bool value = !strcasecmp(inputString.c_str(), "true") || !strcasecmp(inputString.c_str(), "1");
                    if (!value) {
                        if (strcasecmp(inputString.c_str(), "false") && strcasecmp(inputString.c_str(), "0")) {
                            NES_FATAL_ERROR2(
                                "Parser::writeFieldValueToTupleBuffer: Received non boolean value for BOOLEAN field: {}",
                                inputString.c_str());
                            throw std::invalid_argument("Value " + inputString + " is not a boolean");
                        }
                    }
                    tupleBuffer[tupleCount][schemaFieldIndex].write<bool>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UNDEFINED:
                    NES_FATAL_ERROR2("Parser::writeFieldValueToTupleBuffer: Field Type UNDEFINED");
            }
        } else {// char array(string) case
            // obtain pointer from buffer to fill with content via strcpy
            char* value = tupleBuffer[tupleCount][schemaFieldIndex].read<char*>();
            // remove quotation marks from start and end of value (ASSUMES QUOTATIONMARKS AROUND STRINGS)
            // improve behavior with json library
            strcpy(value, inputString.c_str());
        }
    } catch (const std::exception& e) {
        NES_ERROR2("Failed to convert inputString to desired NES data type. Error: {}", e.what());
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

bool checkIfBuffersAreEqual(Runtime::TupleBuffer buffer1, Runtime::TupleBuffer buffer2, const uint64_t schemaSizeInByte) {
    NES_DEBUG2("Checking if the buffers are equal, so if they contain the same tuples...");
    if (buffer1.getNumberOfTuples() != buffer2.getNumberOfTuples()) {
        NES_DEBUG2("Buffers do not contain the same tuples, as they do not have the same number of tuples");
        return false;
    }

    std::set<size_t> sameTupleIndices;
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
            NES_DEBUG2("Buffers do not contain the same tuples, as tuple could not be found in both buffers for idx: {}",
                       idxBuffer1);
            return false;
        }
    }

    return (sameTupleIndices.size() == buffer1.getNumberOfTuples());
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
            auto dataType = field->getDataType();
            auto physicalType = physicalDataTypeFactory.getPhysicalType(dataType);
            auto fieldSize = physicalType->size();
            std::string str;
            auto indexInBuffer = buffer + offset + i * schema->getSchemaSizeInBytes();

            // handle variable-length field
            if (dataType->isText()) {
                NES_DEBUG2("Util::printTupleBufferAsCSV(): trying to read the variable length TEXT field: "
                           "from the tuple buffer");

                // read the child buffer index from the tuple buffer
                Runtime::TupleBuffer::NestedTupleBufferKey childIdx = *reinterpret_cast<uint32_t const*>(indexInBuffer);

                // retrieve the child buffer from the tuple buffer
                auto childTupleBuffer = tbuffer.loadChildBuffer(childIdx);

                // retrieve the size of the variable-length field from the child buffer
                uint32_t sizeOfTextField = *(childTupleBuffer.getBuffer<uint32_t>());

                // build the string
                if (sizeOfTextField > 0) {
                    auto begin = childTupleBuffer.getBuffer() + sizeof(uint32_t);
                    std::string deserialized(begin, begin + sizeOfTextField);
                    str = std::move(deserialized);
                }

                else {
                    NES_WARNING2("Util::printTupleBufferAsCSV(): Variable-length field could not be read. Invalid size in the "
                                 "variable-length TEXT field. Returning an empty string.")
                }
            }

            else {
                str = physicalType->convertRawToString(indexInBuffer);
            }

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

}// namespace NES::Runtime::Execution::Util