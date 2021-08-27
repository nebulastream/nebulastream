
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>

#include <Sources/MQTTSource.hpp>
//#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cstring>

#ifndef NES_INCLUDE_SOURCES_FORMATS_FORMATITERATORS_SOURCEFORMATITERATOR_HPP_
#define NES_INCLUDE_SOURCES_FORMATS_FORMATITERATORS_SOURCEFORMATITERATOR_HPP_
void mqttMessageToNESBuffer(std::basic_string<char> data, int tupleCount, uint64_t tupleSize, const NES::SchemaPtr& schema,
                          std::vector<NES::PhysicalTypePtr> physicalTypes, NES::Runtime::TupleBuffer& buffer,
                          NES::MQTTSourceDescriptor::DataType dataType) {

    NES_DEBUG("TupleCount: " << tupleCount);
    uint64_t offset = 0;

    std::vector<std::string> helperToken;
    std::vector<std::string> values;


    if (dataType == NES::MQTTSourceDescriptor::JSON) {
        // extract values as strings from MQTT message
        std::vector<std::string> keyValuePairs = NES::UtilityFunctions::splitWithStringDelimiter(data, ",");
        for (auto & keyValuePair : keyValuePairs) {
            values.push_back(NES::UtilityFunctions::splitWithStringDelimiter(keyValuePair,":")[1]);
        }

        // iterate over fields of schema and cast string values to correct type
        for (uint64_t j = 0; j < schema->getSize(); j++) {
            auto field = physicalTypes[j];
            uint64_t fieldSize = field->size();

            NES_ASSERT2_FMT(fieldSize + offset + tupleCount * tupleSize < buffer.getBufferSize(),
                            "Overflow detected: buffer size = " << buffer.getBufferSize() << " position = "
                            << (offset + tupleCount * tupleSize) << " field size " << fieldSize);

            //ToDO change according to new memory layout
            //not put into other function because it would cause a function call every loop iteration (except if inlining takes place!?)
            if (field->isBasicType()) {
                NES_ASSERT2_FMT(!values[j].empty(), "Field cannot be empty if basic type");
                auto basicPhysicalField = std::dynamic_pointer_cast<NES::BasicPhysicalType>(field);
                switch (basicPhysicalField->nativeType) {
                    case NES::BasicPhysicalType::INT_8:
                    {
                        auto value = static_cast<int8_t>(std::stoi(values[j]));
                        memcpy(buffer.getBuffer<char>() + offset + tupleCount * tupleSize, &value, fieldSize);
                        break;
                    }
                    case NES::BasicPhysicalType::INT_16:
                    {
                        auto value = static_cast<int16_t>(std::stol(values[j]));
                        memcpy(buffer.getBuffer<char>() + offset + tupleCount * tupleSize, &value, fieldSize);
                        break;
                    }
                    case NES::BasicPhysicalType::INT_32:
                    {
                        auto value = static_cast<int32_t>(std::stol(values[j]));
                        memcpy(buffer.getBuffer<char>() + offset + tupleCount * tupleSize, &value, fieldSize);
                        break;
                    }
                    case NES::BasicPhysicalType::INT_64:
                    {
                        auto value = static_cast<int64_t>(std::stoll(values[j]));
                        memcpy(buffer.getBuffer<char>() + offset + tupleCount * tupleSize, &value, fieldSize);
                        break;
                    }
                    case NES::BasicPhysicalType::UINT_8:
                    {
                        auto value = static_cast<uint8_t>(std::stoi(values[j]));
                        memcpy(buffer.getBuffer<char>() + offset + tupleCount * tupleSize, &value, fieldSize);
                        break;
                    }
                    case NES::BasicPhysicalType::UINT_16:
                    {
                        auto value = static_cast<uint16_t>(std::stoul(values[j]));
                        memcpy(buffer.getBuffer<char>() + offset + tupleCount * tupleSize, &value, fieldSize);
                        break;
                    }
                    case NES::BasicPhysicalType::UINT_32:
                    {
                        auto value = static_cast<uint32_t>(std::stoul(values[j]));
                        memcpy(buffer.getBuffer<char>() + offset + tupleCount * tupleSize, &value, fieldSize);
                        break;
                    }
                    case NES::BasicPhysicalType::UINT_64:
                    {
                        auto value = static_cast<uint64_t>(std::stoull(values[j]));
                        memcpy(buffer.getBuffer<char>() + offset + tupleCount * tupleSize, &value, fieldSize);
                        break;
                    }
                    case NES::BasicPhysicalType::FLOAT:
                    {
                        auto value = static_cast<float>(std::stoi(values[j]));
                        memcpy(buffer.getBuffer<char>() + offset + tupleCount * tupleSize, &value, fieldSize);
                        break;
                    }
                    case NES::BasicPhysicalType::DOUBLE:
                    {
                        auto value = static_cast<double>(std::stoi(values[j]));
                        memcpy(buffer.getBuffer<char>() + offset + tupleCount * tupleSize, &value, fieldSize);
                        break;
                    }
                    case NES::BasicPhysicalType::CHAR:
                    {
                        //verify that only a single char was transmitted
                        if(values[j].size() > 1) {
                            NES_FATAL_ERROR("SourceFormatIterator::mqttMessageToNESBuffer: Received non char Value for CHAR Field " << values[j].c_str());
                            throw std::invalid_argument("Value " + values[j] + " is not a char");
                        }
                        char value = values[j].at(0);
                        memcpy(buffer.getBuffer<char>() + offset + tupleCount * tupleSize, &value, fieldSize);
                        break;
                    }
                    case NES::BasicPhysicalType::BOOLEAN:
                    {
                        //verify that a valid bool was transmitted (valid{true,false,0,1})
                        bool value = !strcasecmp(values[j].c_str(), "true") || !strcasecmp(values[j].c_str(), "1");
                        if(!value) {
                            if(strcasecmp(values[j].c_str(), "false") && strcasecmp(values[j].c_str(), "0")) {
                                NES_FATAL_ERROR("SourceFormatIterator::mqttMessageToNESBuffer: Received non boolean value for BOOLEAN field: " << values[j].c_str());
                                throw std::invalid_argument("Value " + values[j] + " is not a boolean");
                            }
                        }
                        memcpy(buffer.getBuffer<char>() + offset + tupleCount * tupleSize, &value, fieldSize);
                        break;
                    }
                }
            } else { // char array(string) case
                std::string value;
                // remove quotation marks from start and end of value (ASSUMES QUOTATIONMARKS AROUND STRINGS)
                value = values[j].substr(1, values[j].size() - 2);
                memcpy(buffer.getBuffer<char>() + offset + tupleCount * tupleSize, value.c_str(), fieldSize);
            }
            offset += fieldSize;
        }
    }
}
#endif //NES_INCLUDE_SOURCES_FORMATS_FORMATITERATORS_SOURCEFORMATITERATOR_HPP_
