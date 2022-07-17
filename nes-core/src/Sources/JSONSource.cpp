#ifdef ENABLE_SIMDJSON_BUILD
#include "Common/DataTypes/DataType.hpp"
#include "Sources/Parsers/JSONParser.hpp"
#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/JSONSource.hpp>
#include <Sources/Parsers/Parser.hpp>
#include <iostream>
#include <simdjson.h>

namespace NES {

JSONSource::JSONSource(SchemaPtr schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       JSONSourceTypePtr jsonSourceType,
                       OperatorId operatorId,
                       OriginId originId,
                       size_t numSourceLocalBuffers,
                       GatheringMode::Value gatheringMode)
    : DataSource(schema,
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 originId,
                 numSourceLocalBuffers,
                 gatheringMode),
      jsonSourceType(jsonSourceType), filePath(jsonSourceType->getFilePath()->getValue()),
      numBuffersToProcess(jsonSourceType->getNumBuffersToProcess()->getValue()) {

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }
    this->inputParser = std::make_shared<JSONParser>(physicalTypes);

    json = simdjson::padded_string::load(filePath);
    auto error = parser.iterate_many(json).get(documentStream);
    if (error) {
        // TODO
        NES_ERROR("Error reading JSON file")
        throw std::logic_error(error_message(error));
    }
}

std::optional<Runtime::TupleBuffer> JSONSource::receiveData() {
    // TODO verify (is copy and paste from CSVSource)
    NES_TRACE("JSONSource::receiveData called on " << operatorId);
    auto buffer = allocateBuffer();
    fillBuffer(buffer);
    NES_TRACE("JSONSource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());

    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer.getBuffer();
}

void JSONSource::fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) {
    uint64_t numOverwrites = 1;
    auto i = documentStream.begin();
    uint64_t tupleIndex = 0;
    for (; i != documentStream.end(); ++i) {
        auto doc = *i;
        if (!doc.error()) {
            if (tupleIndex >= buffer.getCapacity()) {
                if (numBuffersToProcess == 0) {
                    // read source until buffer is full
                    NES_INFO("Buffer is full but there are still tuples left.")
                    return;
                } else if (numBuffersToProcess == 1) {
                    // read source until source ends and overwrite current buffer content
                    NES_INFO("Overwriting buffer (" << numOverwrites << ")")
                    numOverwrites++;
                    tupleIndex = 0;
                } else {
                    NES_ERROR("Logic not yet implemented")
                    throw std::invalid_argument("numBuffersToProcess must be 0 or 1");
                    // TODO
                }
            }
            for (uint64_t fieldIndex = 0; fieldIndex < schema->getSize(); fieldIndex++) {
                DataTypePtr dataType = schema->fields[fieldIndex]->getDataType();
                std::string jsonKey = schema->fields[fieldIndex]->getName();
                inputParser->writeFieldValueToTupleBuffer(tupleIndex, fieldIndex, schema, doc, buffer);
            }
            tupleIndex++;
            buffer.setNumberOfTuples(tupleIndex);
        } else {
            NES_ERROR("got broken document at " << i.current_index());
            throw std::logic_error(error_message(doc.error()));
        }
    }
}

std::string JSONSource::toString() const {
    std::stringstream ss;
    ss << "JSON_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq=" << this->gatheringInterval.count()
       << "ms)";
    return ss.str();
}

std::string JSONSource::getFilePath() const { return filePath; }
SourceType JSONSource::getType() const { return JSON_SOURCE; }
const JSONSourceTypePtr& JSONSource::getSourceConfig() const { return jsonSourceType; }

}// namespace NES
#endif