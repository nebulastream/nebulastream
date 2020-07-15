#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/DataSink.hpp>
#include <Sinks/FileSink.hpp>
#include <Util/UtilityFunctions.hpp>
#include <fstream>
#include <iostream>
#include <sstream>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <string>
namespace NES {

FileSink::FileSink()
    : DataSink() {
    fileOutputMode = FILE_APPEND;
}

FileSink::FileSink(SchemaPtr schema, SinkFormat format, const std::string filePath,
                   FileOutputMode mode)
    : DataSink(schema) {
    this->filePath = filePath;
    fileOutputMode = mode;
}

const std::string FileSink::toString() const {
    std::stringstream ss;
    ss << "PRINT_SINK(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    return ss.str();
}

void FileSink::setup() {
    NES_DEBUG("FileSink: Creating FileSink: medium " << getMediumAsString() << " format " << getFormatAsString()
                                                     << " and mode " << this->getOutputMode());
}

void FileSink::shutdown() {
    NES_DEBUG("FileSink: Destroying FileSink medium " << getMediumAsString() << " format " << getFormatAsString()
                                                      << " and mode " << this->getOutputMode());
}

bool FileSink::writeSchema() {
    NES_DEBUG("FileSink: writeSchema medium " << getMediumAsString() << " format " << getFormatAsString()
                                              << " and mode " << this->getOutputMode());
    std::ofstream outputFile;

    if (format == JSON_FORMAT) {
        NES_NOT_IMPLEMENTED();
    } else if (format == CSV_FORMAT) {
        outputFile.open(filePath, std::ofstream::out | std::ofstream::trunc);

        std::stringstream ss;
        for (auto& f : schema->fields) {
            ss << f->toString() << ",";
        }
        outputFile << ss.str();
        outputFile.close();
    } else if (format == NES_FORMAT) {
        size_t idx = filePath.rfind(".");
        std::string shrinkedPath = filePath.substr(idx+1);
        std::string schemaFile = shrinkedPath + ".schema";
        outputFile.open(schemaFile, std::ofstream::binary | std::ofstream::trunc);
        SerializableSchema* serializedSchema;
        SerializableSchema* serial = SchemaSerializationUtil::serializeSchema(schema, serializedSchema);
        outputFile.write(reinterpret_cast<char*>(serial), schema->getSchemaSizeInBytes());
    } else {
        NES_ERROR("FileSink::writeSchema: format not supported");
    }
}

bool FileSink::writeData(TupleBuffer& inputBuffer) {
    if (!inputBuffer.isValid()) {
        NES_ERROR("FileSink::writeData input buffer invalid");
        return false;
    }

    std::ofstream outputFile;
    if (fileOutputMode == FILE_APPEND) {
        if (format != NES_FORMAT) {
            outputFile.open(filePath, std::ofstream::out | std::ofstream::app);
        } else {
            outputFile.open(filePath, std::ofstream::binary | std::ofstream::app);
        }
    } else if (fileOutputMode == FILE_OVERWRITE) {
        if (format != NES_FORMAT) {
            outputFile.open(filePath, std::ofstream::out | std::ofstream::trunc);
        } else {
            outputFile.open(filePath, std::ofstream::binary | std::ofstream::trunc);
        }
    } else {
        NES_ERROR("FileOutputSink::writeData: write mode not supported=" << fileOutputMode);
        return false;
    }

    NES_DEBUG("CSVSink::writeData: write buffer tuples " << inputBuffer.getNumberOfTuples() << " in format " << getFormatAsString() << " in file");

    if (format == JSON_FORMAT) {
        NES_NOT_IMPLEMENTED();
    } else if (format == CSV_FORMAT) {
        std::string bufferContent = UtilityFunctions::printTupleBufferAsCSV(inputBuffer, schema);
        outputFile << bufferContent;
    } else if (format == NES_FORMAT) {
        outputFile.write(reinterpret_cast<char*>(inputBuffer.getBuffer()), sizeof(inputBuffer.getBufferSize()));
    } else if (format == TEXT_FORMAT) {
        std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(inputBuffer, schema);
        outputFile << bufferContent;
    } else {
        NES_ERROR("FileSink::writeData: format not supported");
    }

    outputFile.close();
}

SinkMedium FileSink::getType() const {
    return FILE_SINK;
}

const std::string FileSink::getFilePath() const {
    return filePath;
}

FileOutputMode FileSink::getOutputMode() const {
    return fileOutputMode;
}

SinkFormat FileSink::getSinkFormat() const {
    return format;
}

std::string FileSink::getFileOutputModeAsString()
{
    if(fileOutputMode == FILE_OVERWRITE)
    {
        return "FILE_OVERWRITE";
    }
    if(fileOutputMode == FILE_APPEND)
    {
        return "FILE_APPEND";
    }
    else
    {
        return "UNKNOWN fileOutputMode";
    }
}

}// namespace NES
