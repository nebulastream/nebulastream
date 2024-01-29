#include "Common/PhysicalTypes/BasicPhysicalType.hpp"
#include "Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp"
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <CSVDataGenerator.h>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>

bool CSVDataGenerator::parseLineIntoBuffer(const std::string& line, NES::Runtime::MemoryLayouts::DynamicTuple tuple) {
    std::vector<std::string> splits;
    boost::split(splits, line, boost::is_any_of(","));

    for (std::size_t i = 0; i < schema->getSize(); ++i) {
        auto schemaFieldIndex = i;
        auto attr = schema->get(i);
        auto dataType = attr->getDataType();
        auto physicalType = NES::DefaultPhysicalTypeFactory().getPhysicalType(dataType);
        auto inputString = splits[i];

        if (physicalType->isBasicType()) {
            auto basicPhysicalType = std::dynamic_pointer_cast<NES::BasicPhysicalType>(physicalType);
            switch (basicPhysicalType->nativeType) {
                case NES::BasicPhysicalType::NativeType::INT_8: {
                    auto value = static_cast<int8_t>(std::stoi(inputString));
                    tuple[schemaFieldIndex].write<int8_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_16: {
                    auto value = static_cast<int16_t>(std::stol(inputString));
                    tuple[schemaFieldIndex].write<int16_t>(value);

                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_32: {
                    auto value = static_cast<int32_t>(std::stol(inputString));
                    tuple[schemaFieldIndex].write<int32_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_64: {
                    auto value = static_cast<int64_t>(std::stoll(inputString));
                    tuple[schemaFieldIndex].write<int64_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_8: {
                    auto value = static_cast<uint8_t>(std::stoi(inputString));
                    tuple[schemaFieldIndex].write<uint8_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_16: {
                    auto value = static_cast<uint16_t>(std::stoul(inputString));
                    tuple[schemaFieldIndex].write<uint16_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_32: {
                    auto value = static_cast<uint32_t>(std::stoul(inputString));
                    tuple[schemaFieldIndex].write<uint32_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_64: {
                    auto value = static_cast<uint64_t>(std::stoull(inputString));
                    tuple[schemaFieldIndex].write<uint64_t>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::FLOAT: {
                    boost::replace_all(inputString, ",", ".");
                    auto value = static_cast<float>(std::stof(inputString));
                    tuple[schemaFieldIndex].write<float>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::DOUBLE: {
                    auto value = static_cast<double>(std::stod(inputString));
                    tuple[schemaFieldIndex].write<double>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::CHAR: {
                    //verify that only a single char was transmitted
                    if (inputString.size() > 1) {
                        NES_FATAL_ERROR("SourceFormatIterator::mqttMessageToNESBuffer: Received non char Value for CHAR Field {}",
                                        inputString);
                        throw std::invalid_argument("Value " + inputString + " is not a char");
                    }
                    char value = inputString.at(0);
                    tuple[schemaFieldIndex].write<char>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::TEXT: {
                    NES_THROW_RUNTIME_ERROR("Not Implemented");
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
                    tuple[schemaFieldIndex].write<bool>(value);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UNDEFINED:
                    NES_FATAL_ERROR("Parser::writeFieldValueToTupleBuffer: Field Type UNDEFINED");
            }
        } else {// char array(string) case
            NES_THROW_RUNTIME_ERROR("Not Implemented");
        }
    }

    return true;
}

std::vector<NES::Runtime::TupleBuffer> CSVDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::vector<NES::Runtime::TupleBuffer> buffers;
    buffers.reserve(numberOfBuffers);
    bool eof = false;
    auto memoryLayout = getMemoryLayout(bufferSize);

    for (uint64_t currentBuffer = 0; currentBuffer < numberOfBuffers; currentBuffer++) {
        if (eof) {
            break;
        }
        auto tuples_in_buffer = 0;
        auto buffer = allocateBuffer();
        auto dynamicBuffer = NES::Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t currentRecord = 0; currentRecord < dynamicBuffer.getCapacity(); currentRecord++) {
            std::string line;
            std::getline(file, line);
            if (file.eof()) {
                if (repeat) {
                    file.clear();
                    file.seekg(0);
                    std::getline(file, line);
                    NES_ASSERT(!line.empty(), "File is empty");
                    currentRecord--;
                    continue;
                } else {
                    eof = true;
                    break;
                }
            }

            if (!parseLineIntoBuffer(line, dynamicBuffer[currentRecord])) {
                NES_THROW_RUNTIME_ERROR("Failed To Parse line into Tuple");
            }
            tuples_in_buffer++;
        }
        dynamicBuffer.setNumberOfTuples(tuples_in_buffer);
        buffers.push_back(buffer);
    }

    return buffers;
}

NES::SchemaPtr CSVDataGenerator::getSchema() { return schema; }
std::string CSVDataGenerator::getName() { return "CSVSource"; }
std::string CSVDataGenerator::toString() { return "CSVSource(" + schema->toString() + ")"; }

NES::SchemaPtr parseSchema(std::string firstLineOfCSVFile) {
    std::vector<std::string> splits;
    boost::split(splits, firstLineOfCSVFile, boost::is_any_of(","));

    auto schema = NES::Schema::create();

    for (const auto& split : splits) {
        auto index = split.find(':', 0);
        auto name = split.substr(0, index);
        auto type = split.substr(index + 1, split.length());
        schema->addField(name, magic_enum::enum_cast<NES::BasicType>(type).value());
    }

    return schema;
}

std::unique_ptr<CSVDataGenerator> CSVDataGenerator::create(const char* filename, bool repeat, NES::SchemaPtr schema) {
    std::ifstream f(filename);
    if (!f)
        return nullptr;

    if (!schema) {
        std::string first_line;
        std::getline(f, first_line);
        schema = parseSchema(first_line);
    }

    return std::unique_ptr<CSVDataGenerator>(new CSVDataGenerator(std::move(f), std::move(schema), repeat));
}
NES::Configurations::SchemaTypePtr CSVDataGenerator::getSchemaType() { NES_NOT_IMPLEMENTED(); }
