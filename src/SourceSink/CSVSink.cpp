#include <NodeEngine/TupleBuffer.hpp>
#include <SourceSink/CSVSink.hpp>
#include <Util/Logger.hpp>
#include <fstream>
#include <iostream>
#include <string>

namespace NES {
CSVSink::CSVSink(SchemaPtr schema, std::string filepath)
    : FileOutputSink(schema, filepath, FileOutputType::CSV_TYPE, FileOutputMode::FILE_OVERWRITE) {
}

CSVSink::CSVSink(SchemaPtr schema, std::string filePath, FileOutputMode mode)
    : FileOutputSink(schema, filePath, FileOutputType::CSV_TYPE, mode) {
}

bool CSVSink::writeData(TupleBuffer& input_buffer) {
    NES_DEBUG("CSVSink::writeData: write buffer tuples " << input_buffer.getNumberOfTuples());

    std::ofstream outputFile;
    if (outputMode == FILE_APPEND) {
        outputFile.open(filePath, std::ofstream::out | std::ofstream::app);
    } else if (outputMode == FILE_OVERWRITE) {
        outputFile.open(filePath, std::ofstream::out | std::ofstream::trunc);
    } else {
        NES_ERROR("FileOutputSink::writeData: write mode not supported=" << outputMode);
        return false;
    }
    outputFile << this->outputBufferWithSchema(input_buffer, schema);
    outputFile << "test" << std::endl;
    outputFile.flush();
    outputFile.close();

    return true;
}

std::string CSVSink::outputBufferWithSchema(TupleBuffer& tupleBuffer, SchemaPtr schema) const {
    if (!tupleBuffer.isValid()) {
        return "INVALID_BUFFER_PTR";
    }

    std::stringstream ss;
    auto numberOfTuples = tupleBuffer.getNumberOfTuples();
    auto buffer = tupleBuffer.getBufferAs<char>();
    for (size_t i = 0; i < numberOfTuples; i++) {
        size_t offset = 0;
        for (size_t j = 0; j < schema->getSize(); j++) {
            auto field = schema->get(j);
            size_t fieldSize = field->getFieldSize();
            DataTypePtr ptr = field->getDataType();
            std::string str = ptr->convertRawToString(
                buffer + offset + i * schema->getSchemaSizeInBytes());
            ss << str.c_str();
            if (j < schema->getSize() - 1) {
                ss << ",";
            }
            offset += fieldSize;
        }
        ss << std::endl;
    }
    return ss.str();
}

}// namespace NES
