#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <iostream>
#include <string>

namespace NES {

FileSink::FileSink()
    : SinkMedium() {
}

std::string FileSink::toString() {
    return "FILE_SINK";
}

FileSink::FileSink(SchemaPtr schema, SinkFormatPtr format, const std::string filePath, bool append)
    : SinkMedium(schema) {
    this->filePath = filePath;
    this->sinkFormat = format;
    this->append = append;
}

const std::string FileSink::toString() const {
    std::stringstream ss;
    ss << "PRINT_SINK(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    return ss.str();
}

void FileSink::setup() {
}

void FileSink::shutdown() {
}

bool FileSink::writeData(TupleBuffer& inputBuffer) {
    NES_DEBUG("FileSink: writeSchema medium " << toString() << " format " << sinkFormat->toString()
                                              << " and mode " << this->getAppendAsString());

    if (!inputBuffer.isValid()) {
        NES_ERROR("FileSink::writeData input buffer invalid");
        return false;
    }
    if (!schemaWritten) {
        NES_DEBUG("FileSink::writeData: write schema");
        sinkFormat->writeSchema();
        schemaWritten = true;
        NES_DEBUG("FileSink::writeData: write written");

    } else {
        NES_DEBUG("FileSink::writeData: schema already written");
    }

    NES_DEBUG("FileSink::writeData: write data");
    bool success = sinkFormat->writeData(inputBuffer);

    if (!success) {
        NES_ERROR("FileSink::writeData: writing data failed");
        return false;
    } else {
        NES_DEBUG("FileSink::writeData: writing data succeed");
        return true;
    }
}

const std::string FileSink::getFilePath() const {
    return filePath;
}

}// namespace NES
