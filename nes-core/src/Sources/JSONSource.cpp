
#ifdef ENABLE_SIMDJSON_BUILD
#include "Sources/Parsers/JSONParser.hpp"
#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/JSONSource.hpp>
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
      jsonSourceType(jsonSourceType), filePath(jsonSourceType->getFilePath()->getValue()) {

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }
    this->inputParser = std::make_shared<JSONParser>(physicalTypes);
}

std::optional<Runtime::TupleBuffer> JSONSource::receiveData() {
    NES_TRACE("JSONSource::receiveData called on " << operatorId);
    auto buffer = allocateBuffer();
    fillBuffer(buffer);
    NES_TRACE("JSONSource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());

    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer.getBuffer();
    ;
}

void JSONSource::fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) {
    simdjson::ondemand::document_stream docs = parser.iterate_many(filePath);
    uint64_t tupleCount = 0;
    for (auto doc : docs) {
        simdjson::ondemand::document_reference docRef = doc.value();
        inputParser->writeInputTupleToTupleBuffer(docRef, tupleCount, buffer, schema); // TODO don't have to pass schema each time
        tupleCount++;
    }
}

std::string JSONSource::toString() const {
    std::stringstream ss;
    ss << "JSON_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq=" << this->gatheringInterval.count()
       << "ms)";
    return ss.str();
}

SourceType JSONSource::getType() const { return JSON_SOURCE; }

const JSONSourceTypePtr& JSONSource::getSourceConfig() const { return jsonSourceType; }

}// namespace NES
#endif