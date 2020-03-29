#include <SourceSink/CSVSource.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <assert.h>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <Util/Logger.hpp>
#include <boost/algorithm/string.hpp>
#include <SourceSink/DataSource.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(NES::CSVSource);

namespace NES {

CSVSource::CSVSource()
    :
    filePath(""),
    tupleSize(0),
    delimiter(","),
    currentPosInFile(0) {
}

CSVSource::CSVSource(const Schema &schema, const std::string &_file_path,
                     const std::string &delimiter, size_t numBuffersToProcess,
                     size_t frequency)
    :
    DataSource(schema),
    filePath(_file_path),
    delimiter(delimiter),
    currentPosInFile(0) {
  this->numBuffersToProcess = numBuffersToProcess;
  this->gatheringInterval = frequency;
  tupleSize = schema.getSchemaSize();
  NES_DEBUG(
      "CSVSource: tupleSize=" << tupleSize << " freq=" << this->gatheringInterval << " numBuff=" << this->numBuffersToProcess)
}

TupleBufferPtr CSVSource::receiveData() {
  NES_DEBUG("CSVSource::receiveData called")
  TupleBufferPtr buf = BufferManager::instance().getFixSizeBuffer();
  fillBuffer(buf);
  NES_DEBUG(
      "CSVSource::receiveData filled buffer with tuples=" << buf->getNumberOfTuples())
  return buf;
}

const std::string CSVSource::toString() const {
  std::stringstream ss;
  ss << "CSV_SOURCE(SCHEMA(" << schema.toString() << "), FILE=" << filePath
     << " freq=" << this->gatheringInterval << " numBuff="
     << this->numBuffersToProcess << ")";
  return ss.str();
}

void CSVSource::fillBuffer(TupleBufferPtr buf) {
  std::ifstream input(filePath.c_str());
  NES_DEBUG("CSVSource::fillBuffer: read buffer")
  input.seekg(0, input.end);
  int file_size = input.tellg();
  if (file_size == -1) {
    NES_ERROR("CSVSource::fillBuffer File " << filePath << " is corrupted")
    assert(0);
  }
  NES_DEBUG("CSVSource::fillBuffer: start at pos=" << currentPosInFile << " fileSize=" << file_size)
  input.seekg(currentPosInFile, input.beg);

  uint64_t generated_tuples_this_pass = buf->getBufferSizeInBytes() / tupleSize;

  std::string line;
  uint64_t i = 0;
  while (i < generated_tuples_this_pass) {
    if (input.tellg() >= file_size || input.tellg() == -1) {
      NES_DEBUG("CSVSource::fillBuffer: reset tellg()=" << input.tellg() << " file_size=" << file_size)
      input.clear();
      input.seekg(0, input.beg);
    }

    std::getline(input, line);
    NES_DEBUG("CSVSource line=" << i << " val=" << line);
    std::vector<std::string> tokens;
    boost::algorithm::split(tokens, line, boost::is_any_of(this->delimiter));
    size_t offset = 0;
    for (size_t j = 0; j < schema.getSize(); j++) {
      auto field = schema[j];
      size_t fieldSize = field->getFieldSize();

      //TODO: add all other data types here
      if (field->getDataType()->toString() == "UINT64") {
        size_t val = std::stoull(tokens[j].c_str());
        memcpy((char*) buf->getBuffer() + offset + i * tupleSize, &val,
               fieldSize);
      } else if (field->getDataType()->toString() == "FLOAT32") {
        float val = std::stof(tokens[j].c_str());
        memcpy((char*) buf->getBuffer() + offset + i * tupleSize, &val,
               fieldSize);
      } else {
        memcpy((char*) buf->getBuffer() + offset + i * tupleSize,
               tokens[j].c_str(), fieldSize);
      }

      offset += fieldSize;
    }
    i++;
  }

  currentPosInFile = input.tellg();

  NES_DEBUG(
      "CSVSource::fillBuffer: readin finished read " << generated_tuples_this_pass << " tuples at posInFile=" << currentPosInFile)
  generatedTuples += generated_tuples_this_pass;
  buf->setNumberOfTuples(generated_tuples_this_pass);
  generatedBuffers++;
}

SourceType CSVSource::getType() const {
  return CSV_SOURCE;
}
}  // namespace NES
