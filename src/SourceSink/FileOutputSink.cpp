#include <NodeEngine/TupleBuffer.hpp>
#include <SourceSink/DataSink.hpp>
#include <SourceSink/FileOutputSink.hpp>
#include <iostream>
#include <string>

namespace NES {

FileOutputSink::FileOutputSink()
    : DataSink() {
    outputType = BINARY_TYPE;
    outputMode = FILE_APPEND;
}

FileOutputSink::FileOutputSink(const std::string filePath)
    : DataSink() {
    this->filePath = filePath;
    outputType = BINARY_TYPE;
    outputMode = FILE_APPEND;
}

FileOutputSink::FileOutputSink(SchemaPtr schema, std::string filePath)
    : DataSink(schema) {
    this->filePath = filePath;
    outputType = BINARY_TYPE;
    outputMode = FILE_APPEND;
}

FileOutputSink::FileOutputSink(SchemaPtr schema, const std::string filePath,
                               FileOutputType type, FileOutputMode mode)
    : DataSink(schema) {
    this->filePath = filePath;
    outputType = type;
    outputMode = mode;
}

const std::string FileOutputSink::toString() const {
    std::stringstream ss;
    ss << "PRINT_SINK(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    return ss.str();
}

void FileOutputSink::setup() {
    NES_DEBUG("FileOutputSink: Creating FileOutputSink of type " << this->getOutputType()
                                                                 << " and mode " << this->getOutputMode());
}

void FileOutputSink::shutdown() {
    NES_DEBUG("FileOutputSink: Destroying FileOutputSink of type " << this->getOutputType()
                                                                   << " and mode " << this->getOutputMode());
}

SinkType FileOutputSink::getType() const {
    return FILE_SINK;
}
const std::string FileOutputSink::getFilePath() const {
    return filePath;
}
FileOutputSink::FileOutputType FileOutputSink::getOutputType() const {
    return outputType;
}
FileOutputSink::FileOutputMode FileOutputSink::getOutputMode() const {
    return outputMode;
}

}// namespace NES
