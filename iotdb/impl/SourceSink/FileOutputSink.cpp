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
    :
    DataSink() {
  outputType = BINARY_TYPE;
}
FileOutputSink::FileOutputSink(const Schema &schema)
    :
    DataSink(schema) {
  outputType = BINARY_TYPE;
}
FileOutputSink::FileOutputSink(const std::string filePath)
    :
    DataSink() {
  this->filePath = filePath;
  outputType = BINARY_TYPE;
}
FileOutputSink::FileOutputSink(const Schema &schema, const std::string filePath,
                               FileOutPutType pType)
    :
    DataSink(schema) {
  this->filePath = filePath;
  outputType = pType;
}

bool FileOutputSink::writeData(const TupleBufferPtr input_buffer) {

  NES_DEBUG(
      "FileOutputSink::writeData: write bufffer tuples " << input_buffer->getNumberOfTuples())

  if (outputType == BINARY_TYPE) {
    std::fstream outputFile(
        filePath, std::fstream::in | std::fstream::out | std::fstream::app);
    outputFile << NES::toString(input_buffer.get(), this->getSchema());
    outputFile.close();
  } else if (outputType == CSV_TYPE) {
    std::ofstream outputFile;
    outputFile.open(filePath);
    for (size_t i = 0; i < input_buffer->getNumberOfTuples(); i++) {

      size_t offset = 0;

      for (size_t j = 0; j < schema.getSize(); j++) {
        auto field = schema[j];
        size_t fieldSize = field->getFieldSize();
        DataTypePtr ptr = field->getDataType();
        std::string str = ptr->convertRawToString(
            input_buffer->getBuffer() + offset + i * schema.getSchemaSize());
//        NES_DEBUG(
//            "FileOutputSink::writeData: str=" << str << " i=" << i << " j=" << j << " fieldSize=" << fieldSize << " offset=" << offset);
        outputFile << str.c_str();
        if (j < schema.getSize() - 1) {
          outputFile << ",";
        }
        offset += fieldSize;
      }
      outputFile << std::endl;
    }
  }

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
