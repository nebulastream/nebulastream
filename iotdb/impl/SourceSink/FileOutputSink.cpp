#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <iostream>
#include <fstream>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <SourceSink/FileOutputSink.hpp>
#include <SourceSink/DataSink.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(NES::FileOutputSink);

namespace NES {

FileOutputSink::FileOutputSink()
        :
        DataSink() {
    outputType = BINARY_TYPE;
    outputMode = FILE_APPEND;
}

FileOutputSink::FileOutputSink(const std::string filePath)
        :
        DataSink() {
    this->filePath = filePath;
    outputType = BINARY_TYPE;
    outputMode = FILE_APPEND;
}

FileOutputSink::FileOutputSink(SchemaPtr schema, std::string filePath)
        :
        DataSink(schema) {
    this->filePath = filePath;
    outputType = BINARY_TYPE;
    outputMode = FILE_APPEND;
}

FileOutputSink::FileOutputSink(SchemaPtr schema, const std::string filePath,
                               FileOutPutType type, FileOutPutMode mode)
        :
        DataSink(schema) {
    this->filePath = filePath;
    outputType = type;
    outputMode = mode;
}

bool FileOutputSink::writeData(TupleBuffer& input_buffer) {

    NES_DEBUG("FileOutputSink::writeData: write bufffer tuples " << input_buffer.getNumberOfTuples())

    if (outputType == BINARY_TYPE) {
        std::fstream outputFile(filePath, std::fstream::in | std::fstream::out | std::fstream::app);
        outputFile << NES::toString(input_buffer, this->getSchema());
        outputFile.close();
    }
    else if (outputType == CSV_TYPE) {
        std::ofstream outputFile;
        if (outputMode == FILE_APPEND) {
            outputFile.open(filePath, std::ofstream::out | std::ofstream::app);
        } else if (outputMode == FILE_OVERWRITE) {
            outputFile.open(filePath, std::ofstream::out | std::ofstream::trunc);
        } else {
            NES_ERROR("FileOutputSink::writeData: write mode not supported=" << outputMode)
        }
        outputFile << input_buffer.printTupleBuffer(schema);
        outputFile.close();
    }
    return true;
}

const std::string FileOutputSink::toString() const {
    std::stringstream ss;
    ss << "PRINT_SINK(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    return ss.str();
}

void FileOutputSink::setup() {

}

void FileOutputSink::shutdown() {

}

SinkType FileOutputSink::getType() const {
    return FILE_SINK;
}

}  // namespace NES
