#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
#include <Util/Logger.hpp>
#include <assert.h>
#include <boost/algorithm/string.hpp>
#include <sstream>
#include <string>
#include <vector>

namespace NES {

CSVSource::CSVSource()
    : filePath(""),
      tupleSize(0),
      delimiter(","),
      currentPosInFile(0) {
}

CSVSource::CSVSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& _file_path,
                     const std::string& delimiter, size_t numBuffersToProcess,
                     size_t frequency)
    : DataSource(schema, bufferManager, queryManager),
      filePath(_file_path),
      delimiter(delimiter),
      currentPosInFile(0) {
    this->numBuffersToProcess = numBuffersToProcess;
    this->gatheringInterval = frequency;
    tupleSize = schema->getSchemaSizeInBytes();
    NES_DEBUG(
        "CSVSource: tupleSize=" << tupleSize << " freq=" << this->gatheringInterval << " numBuff=" << this->numBuffersToProcess);
}

std::optional<TupleBuffer> CSVSource::receiveData() {
    NES_DEBUG("CSVSource::receiveData called");
    auto buf = this->bufferManager->getBufferBlocking();
    fillBuffer(buf);
    NES_DEBUG(
        "CSVSource::receiveData filled buffer with tuples=" << buf.getNumberOfTuples());
    return buf;
}

const std::string CSVSource::toString() const {
    std::stringstream ss;
    ss << "CSV_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath
       << " freq=" << this->gatheringInterval << " numBuff="
       << this->numBuffersToProcess << ")";
    return ss.str();
}

void CSVSource::fillBuffer(TupleBuffer& buf) {
    std::ifstream input(filePath.c_str());
    NES_DEBUG("CSVSource::fillBuffer: read buffer");
    input.seekg(0, input.end);
    int file_size = input.tellg();
    if (file_size == -1) {
        NES_ERROR("CSVSource::fillBuffer File " << filePath << " is corrupted");
        assert(0);
    }
    NES_DEBUG("CSVSource::fillBuffer: start at pos=" << currentPosInFile << " fileSize=" << file_size);
    input.seekg(currentPosInFile, input.beg);

    uint64_t generated_tuples_this_pass = buf.getBufferSize() / tupleSize;

    std::string line;
    uint64_t i = 0;
    std::vector<PhysicalTypePtr> physicalTypes;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (auto field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }
    while (i < generated_tuples_this_pass) {
        if (input.tellg() >= file_size || input.tellg() == -1) {
            NES_DEBUG("CSVSource::fillBuffer: reset tellg()=" << input.tellg() << " file_size=" << file_size);
            input.clear();
            input.seekg(0, input.beg);
        }

        std::getline(input, line);
        NES_DEBUG("CSVSource line=" << i << " val=" << line);
        std::vector<std::string> tokens;
        boost::algorithm::split(tokens, line, boost::is_any_of(this->delimiter));
        size_t offset = 0;
        for (size_t j = 0; j < schema->getSize(); j++) {
            auto field = physicalTypes[j];
            size_t fieldSize = field->size();

            if (field->isBasicType()) {
                auto basicPhysicalField = std::dynamic_pointer_cast<BasicPhysicalType>(field);
                /*
             * TODO: this requires proper MIN / MAX size checks, numeric_limits<T>-like
             * TODO: this requires underflow/overflow checks
             * TODO: our types need their own sto/strto methods
             */
                if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_64) {
                    uint64_t val = std::stoull(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + i * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_64) {
                    int64_t val = std::stoll(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + i * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_32) {
                    uint32_t val = std::stoul(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + i * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_32) {
                    int32_t val = std::stol(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + i * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_16) {
                    uint16_t val = std::stol(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + i * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_16) {
                    int16_t val = std::stol(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + i * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_16) {
                    uint8_t val = std::stoi(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + i * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_8) {
                    int8_t val = std::stoi(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + i * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_8) {
                    int8_t val = std::stoi(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + i * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::DOUBLE) {
                    double val = std::stod(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + i * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::FLOAT) {
                    float val = std::stof(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + i * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::BOOLEAN) {
                    bool val = (strcasecmp(tokens[j].c_str(), "true") == 0 || atoi(tokens[j].c_str()) != 0);
                    memcpy(buf.getBufferAs<char>() + offset + i * tupleSize, &val,
                           fieldSize);
                }
            } else {
                memcpy(buf.getBufferAs<char>() + offset + i * tupleSize,
                       tokens[j].c_str(), fieldSize);
            }

            offset += fieldSize;
        }
        i++;
    }

    currentPosInFile = input.tellg();

    NES_DEBUG(
        "CSVSource::fillBuffer: readin finished read " << generated_tuples_this_pass << " tuples at posInFile="
                                                       << currentPosInFile);
    generatedTuples += generated_tuples_this_pass;
    buf.setNumberOfTuples(generated_tuples_this_pass);
    generatedBuffers++;
}

SourceType CSVSource::getType() const {
    return CSV_SOURCE;
}

const std::string CSVSource::getFilePath() const {
    return filePath;
}

const std::string CSVSource::getDelimiter() const {
    return delimiter;
}
}// namespace NES
