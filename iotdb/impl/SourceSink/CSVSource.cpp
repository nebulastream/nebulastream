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
    delimiter(",") {
}

CSVSource::CSVSource(const Schema &schema, const std::string &_file_path,
                     const std::string &delimiter, size_t numBuffersToProcess)
    :
    DataSource(schema),
    filePath(_file_path),
    delimiter(delimiter) {
  this->numBuffersToProcess = numBuffersToProcess;
  tupleSize = schema.getSchemaSize();
  NES_DEBUG("CSVSource: tupleSize=" << tupleSize)
}

TupleBufferPtr CSVSource::receiveData() {
  NES_DEBUG("CSVSource::receiveData called")
  TupleBufferPtr buf = BufferManager::instance().getBuffer();
  fillBuffer(buf);
  NES_DEBUG(
      "CSVSource::receiveData filled buffer with tuples=" << buf->getNumberOfTuples())
  return buf;
}

const std::string CSVSource::toString() const {
  std::stringstream ss;
  ss << "CSV_SOURCE(SCHEMA(" << schema.toString() << "), FILE=" << filePath
     << ")";
  return ss.str();
}

void CSVSource::fillBuffer(TupleBufferPtr buf) {
  /** while(generated_tuples < num_tuples_to_process)
   read <buf.buffer_size> bytes data from file into buffer
   advance internal file pointer, if we reach the file end, set to file begin
   */
  std::ifstream input(filePath.c_str());

  input.seekg(0, input.end);
  int file_size = input.tellg();
  if (file_size == -1) {
    NES_ERROR("ERROR: File " << filePath << " is corrupted")
    assert(0);
  }
  input.seekg(0, input.beg);

  uint64_t generated_tuples_this_pass = buf->getBufferSizeInBytes() / tupleSize;

  std::string line;
  std::vector<std::string> tokens;
  uint64_t i = 0;
  while (i < generated_tuples_this_pass) {
    if (input.tellg() >= file_size || input.tellg() == -1) {
      input.clear();
      input.seekg(0, input.beg);
      std::cout << "reset" << std::endl;
    }
    else
    {
      std::cout << "tellg:" << input.tellg() << " size=" << file_size << std::endl;
    }
    std::getline(input, line);
    boost::algorithm::split(tokens, line, boost::is_any_of(this->delimiter));
    size_t offset = 0;
    for (size_t j = 0; j < schema.getSize(); j++) {
      auto field = schema[j];
      std::cout << field->toString() << ": " << tokens[j] << ", ";
      size_t field_size = field->getFieldSize();
      memcpy((char*) buf->getBuffer() + offset + i * tupleSize,
             tokens[j].c_str(), field_size);
      offset += field_size;
    }
    std::cout << std::endl;
    i++;
  }
  generatedTuples += generated_tuples_this_pass;
  buf->setNumberOfTuples(generated_tuples_this_pass);
  generatedBuffers++;
}

SourceType CSVSource::getType() const {
  return CSV_SOURCE;
}
}  // namespace NES
