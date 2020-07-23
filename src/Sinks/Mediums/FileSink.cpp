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
    //TODO add lock?

    NES_DEBUG("FileSink: getSchema medium " << toString() << " format " << sinkFormat->toString()
                                            << " and mode " << this->getAppendAsString());

    if (!inputBuffer.isValid()) {
        NES_ERROR("FileSink::writeData input buffer invalid");
        return false;
    }
    if (!schemaWritten) {//TODO:atomic
        NES_DEBUG("FileSink::getData: write schema");
        auto schemaBuffer = sinkFormat->getSchema();
        if (schemaBuffer) {
            std::ofstream outputFile;
            outputFile.open(filePath, std::ofstream::binary | std::ofstream::trunc);

            outputFile.write((char*) schemaBuffer->getBuffer(), schemaBuffer->getNumberOfTuples());
            NES_DEBUG("CsvFormat::writeData: schema is =" << schema->toString());
            outputFile.close();

            schemaWritten = true;
            NES_DEBUG("FileSink::writeData: write written");
        }
        {
            NES_DEBUG("FileSink::writeData: no schema written");
        }
    } else {
        NES_DEBUG("FileSink::getData: schema already written");
    }

    NES_DEBUG("FileSink::getData: write data");
    auto dataBuffers = sinkFormat->getData(inputBuffer);
    std::ofstream outputFile;
    if (append) {
        NES_DEBUG("file appending in path=" << filePath);
        outputFile.open(filePath, std::ofstream::binary | std::ofstream::app);
    } else {
        NES_DEBUG("file overwriting in path=" << filePath);
        outputFile.open(filePath, std::ofstream::binary | std::ofstream::trunc);
    }
    for (auto buffer : dataBuffers) {
        NES_DEBUG("FileSink::getData: write buffer of size " << buffer.getNumberOfTuples());
        if (sinkFormat->getSinkFormat() == NES_FORMAT) {
            outputFile.write((char*) buffer.getBuffer(), buffer.getNumberOfTuples() * schema->getSchemaSizeInBytes());
        } else {
            outputFile.write((char*) buffer.getBuffer(), buffer.getNumberOfTuples());
        }
    }
    outputFile.close();
    return true;
}

const std::string FileSink::getFilePath() const {
    return filePath;
}

}// namespace NES
