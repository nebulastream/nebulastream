#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <assert.h>
#include <boost/algorithm/string.hpp>
#include <sstream>
#include <string>
#include <vector>
namespace NES {

CSVSource::CSVSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string filePath,
                     const std::string delimiter, size_t numberOfTuplesToProducePerBuffer, size_t numBuffersToProcess,
                     size_t frequency, bool endlessRepeat)
    : DataSource(schema, bufferManager, queryManager),
      filePath(filePath),
      delimiter(delimiter),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer),
      currentPosInFile(0),
      endlessRepeat(endlessRepeat) {
    this->numBuffersToProcess = numBuffersToProcess;
    this->gatheringInterval = frequency;
    tupleSize = schema->getSchemaSizeInBytes();
    NES_DEBUG(
        "CSVSource: tupleSize=" << tupleSize << " freq=" << this->gatheringInterval << " numBuff=" << this->numBuffersToProcess << " numberOfTuplesToProducePerBuffer=" << numberOfTuplesToProducePerBuffer
                                << "endlessRepeat=" << endlessRepeat);

    input.open(filePath.c_str());
    NES_DEBUG("CSVSource::fillBuffer: read buffer");
    input.seekg(0, input.end);
    fileSize = input.tellg();
    if (fileSize == -1) {
        NES_FATAL_ERROR("CSVSource::fillBuffer File " << filePath << " is corrupted");
    }

    fileEnded = false;
}

std::optional<TupleBuffer> CSVSource::receiveData() {
    NES_DEBUG("CSVSource::receiveData called");
    auto buf = this->bufferManager->getBufferBlocking();
    fillBuffer(buf);
    NES_DEBUG(
        "CSVSource::receiveData filled buffer with tuples=" << buf.getNumberOfTuples());
    buf.setWatermark(this->watermark->getWatermark());
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
    NES_DEBUG("CSVSource::fillBuffer: start at pos=" << currentPosInFile << " fileSize=" << fileSize);
    if (fileEnded) {
        NES_WARNING("CSVSource::fillBuffer: but file has already ended");
        buf.setNumberOfTuples(0);
        generatedBuffers++;
        return;
    }
    input.seekg(currentPosInFile, input.beg);

    size_t generated_tuples_this_pass;
    //fill buffer maximally
    if (numberOfTuplesToProducePerBuffer == 0) {
        generated_tuples_this_pass = buf.getBufferSize() / tupleSize;
    } else {
        generated_tuples_this_pass = numberOfTuplesToProducePerBuffer;
    }
    NES_DEBUG("CSVSource::fillBuffer: fill buffer with #tuples=" << generated_tuples_this_pass);

    std::string line;
    uint64_t tupCnt = 0;
    std::vector<PhysicalTypePtr> physicalTypes;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (auto field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }
    while (tupCnt < generated_tuples_this_pass) {
        if (input.tellg() >= fileSize || input.tellg() == -1) {
            NES_DEBUG("CSVSource::fillBuffer: reset tellg()=" << input.tellg() << " file_size=" << fileSize);
            input.clear();
            input.seekg(0, input.beg);
            if (!endlessRepeat) {
                NES_DEBUG("CSVSource::fillBuffer: break because file ended");
                fileEnded = true;
                break;
            }
        }

        std::getline(input, line);
        NES_DEBUG("CSVSource line=" << tupCnt << " val=" << line);
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
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_64) {
                    int64_t val = std::stoll(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_32) {
                    uint32_t val = std::stoul(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_32) {
                    int32_t val = std::stol(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_16) {
                    uint16_t val = std::stol(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_16) {
                    int16_t val = std::stol(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_16) {
                    uint8_t val = std::stoi(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_8) {
                    int8_t val = std::stoi(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_8) {
                    int8_t val = std::stoi(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::DOUBLE) {
                    double val = std::stod(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::FLOAT) {
                    float val = std::stof(tokens[j].c_str());
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val,
                           fieldSize);
                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::BOOLEAN) {
                    bool val = (strcasecmp(tokens[j].c_str(), "true") == 0 || atoi(tokens[j].c_str()) != 0);
                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val,
                           fieldSize);
                }
            } else {
                memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize,
                       tokens[j].c_str(), fieldSize);
            }

            offset += fieldSize;
        }
        tupCnt++;
    }
    currentPosInFile = input.tellg();
    buf.setNumberOfTuples(tupCnt);
    NES_DEBUG(
        "CSVSource::fillBuffer: reading finished read " << tupCnt << " tuples at posInFile="
                                                        << currentPosInFile);
    NES_DEBUG("CSVSource::fillBuffer: read produced buffer= " << UtilityFunctions::printTupleBufferAsCSV(buf, schema));

    //update statistics
    generatedTuples += tupCnt;
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

const size_t CSVSource::getNumberOfTuplesToProducePerBuffer() const {
    return numberOfTuplesToProducePerBuffer;
}
bool CSVSource::isEndlessRepeat() const {
    return endlessRepeat;
}
void CSVSource::setEndlessRepeat(bool endlessRepeat) {
    this->endlessRepeat = endlessRepeat;
}
}// namespace NES
