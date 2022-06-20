
#ifdef ENABLE_SIMDJSON_BUILD
#include <Sources/DataSource.hpp>
#include <Sources/JSONSource.hpp>
#include <iostream>
#include <simdjson.h>

namespace NES {

JSONSource::JSONSource(SchemaPtr schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
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
                 gatheringMode) {
    // TODO
}
std::optional<Runtime::TupleBuffer> JSONSource::receiveData() { return std::optional<Runtime::TupleBuffer>(); }

std::string JSONSource::toString() const { return std::string(); }

SourceType JSONSource::getType() const { return STATIC_DATA_SOURCE; }

std::tuple<int64_t, std::string> JSONSource::parse(const std::string& filepath) {
    simdjson::ondemand::parser parser;
    simdjson::padded_string json = simdjson::padded_string::load(filepath);
    simdjson::ondemand::document doc = parser.iterate(json);
    int64_t value1 = doc.find_field("key1");
    std::string_view value2 = doc.find_field("key2");
    std::string value2_string = {value2.begin(), value2.end()};
    std::cout << "key1: " << value1 << "\nkey2: " << value2 << " - " << value2_string << std::endl;
    return std::make_tuple(value1, value2_string);
}

}// namespace NES
#endif