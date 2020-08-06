#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <iostream>
#include <string>
#include <utility>

namespace NES {

std::string FileSink::toString() {
    return "FILE_SINK";
}

SinkMediumTypes FileSink::getSinkMediumType() {
    return FILE_SINK;
}

FileSink::FileSink(SinkFormatPtr format, const std::string filePath, bool append)
    : SinkMedium(std::move(format)) {
    this->filePath = filePath;
    this->append = append;
    if (!append) {
        int success = std::remove(filePath.c_str());
        NES_DEBUG("FileSink: remove existing file=" << success);
    }
    NES_DEBUG("FileSink: open file=" << filePath);
    if (!outputFile.is_open()) {
        outputFile.open(filePath, std::ofstream::binary | std::ofstream::app);
    }
}
FileSink::~FileSink() {
    NES_DEBUG("~FileSink: close file=" << filePath);
    outputFile.close();
}

const std::string FileSink::toString() const {
    std::stringstream ss;
    ss << "FileSink(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << "), ";
    return ss.str();
}

void FileSink::setup() {
}

void FileSink::shutdown() {
}

bool FileSink::writeData(TupleBuffer& inputBuffer) {
    std::unique_lock lock(writeMutex);

    NES_DEBUG("FileSink: getSchema medium " << toString() << " format " << sinkFormat->toString()
                                            << " and mode " << this->getAppendAsString());

    if (!inputBuffer.isValid()) {
        NES_ERROR("FileSink::writeData input buffer invalid");
        return false;
    }
    if (!schemaWritten) {
        NES_DEBUG("FileSink::getData: write schema");
        auto schemaBuffer = sinkFormat->getSchema();
        if (schemaBuffer) {
            std::ofstream outputFile;
            if (sinkFormat->getSinkFormat() == NES_FORMAT) {
                size_t idx = filePath.rfind(".");
                std::string shrinkedPath = filePath.substr(0, idx + 1);
                std::string schemaFile = shrinkedPath + "schema";
                NES_DEBUG("FileSink::writeData: schema is =" << sinkFormat->getSchemaPtr()->toString() << " to file=" << schemaFile);
                outputFile.open(schemaFile, std::ofstream::binary | std::ofstream::trunc);
            } else {
                outputFile.open(filePath, std::ofstream::binary | std::ofstream::trunc);
            }

            outputFile.write((char*) schemaBuffer->getBuffer(), schemaBuffer->getNumberOfTuples());
            outputFile.close();

            schemaWritten = true;
            NES_DEBUG("FileSink::writeData: schema written");
        } else {
            NES_DEBUG("FileSink::writeData: no schema written");
        }
    } else {
        NES_DEBUG("FileSink::getData: schema already written");
    }

    NES_DEBUG("FileSink::getData: write data to file=" << filePath);
    auto dataBuffers = sinkFormat->getData(inputBuffer);

    for (auto buffer : dataBuffers) {
        NES_DEBUG("FileSink::getData: write buffer of size " << buffer.getNumberOfTuples());
        if (sinkFormat->getSinkFormat() == NES_FORMAT) {
            outputFile.write((char*) buffer.getBuffer(), buffer.getNumberOfTuples() * sinkFormat->getSchemaPtr()->getSchemaSizeInBytes());
        } else {
            outputFile.write((char*) buffer.getBuffer(), buffer.getNumberOfTuples());
        }
    }
    outputFile.flush();

    return true;
}

const std::string FileSink::getFilePath() const {
    return filePath;
}

}// namespace NES
