#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <iostream>
#include <fstream>

#include <Util/Logger.hpp>
#include <SourceSink/FileOutputSink.hpp>
#include <SourceSink/DataSink.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(NES::FileOutputSink);

namespace NES {

FileOutputSink::FileOutputSink()
    : DataSink() {
}
FileOutputSink::FileOutputSink(const Schema& schema)
    : DataSink(schema) {
}
FileOutputSink::FileOutputSink(const std::string filePath) : DataSink() {
    this->filePath = filePath;
}
FileOutputSink::FileOutputSink(const Schema& schema, const std::string filePath) : DataSink(schema) {
    this->filePath = filePath;
}

bool FileOutputSink::writeData(const TupleBufferPtr input_buffer) {

    std::fstream outputFile(filePath, std::fstream::in | std::fstream::out | std::fstream::app);
    outputFile << NES::toString(input_buffer.get(), this->getSchema());
    outputFile.close();
    return true;
}

const std::string FileOutputSink::toString() const {
    std::stringstream ss;
    ss << "PRINT_SINK(";
    ss << "SCHEMA(" << schema.toString() << "), ";
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
