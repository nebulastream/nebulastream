#include <NodeEngine/Dispatcher.hpp>
#include <assert.h>
#include <fstream>
#include <sstream>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>

#include <SourceSink/BinarySource.hpp>
#include <SourceSink/DataSource.hpp>
#include <Util/Logger.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(NES::BinarySource);

namespace NES {

BinarySource::BinarySource()
    : file_path(""),
      file_size(0),
      tuple_size(0) {
}

BinarySource::BinarySource(SchemaPtr schema, const std::string& _file_path)
    : DataSource(schema),
      input(std::ifstream(_file_path.c_str())),
      file_path(_file_path) {
  input.seekg(0, input.end);
  file_size = input.tellg();
  if (file_size == -1) {
    NES_ERROR("ERROR: File " << _file_path << " is corrupted");
    assert(0);
  }
  input.seekg(0, input.beg);
  tuple_size = schema->getSchemaSizeInBytes();
}

TupleBufferPtr BinarySource::receiveData() {
  TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
  fillBuffer(*buf);
  return buf;
}

const std::string BinarySource::toString() const {
  std::stringstream ss;
  ss << "BINARY_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << file_path
     << ")";
  return ss.str();
}

void BinarySource::fillBuffer(TupleBuffer& buf) {
  /** while(generated_tuples < num_tuples_to_process)
   * read <buf.buffer_size> bytes data from file into buffer
   * advance internal file pointer, if we reach the file end, set to file begin
   */

  if (input.tellg() == file_size) {
    input.seekg(0, input.beg);
  }
  size_t size_to_read =
      buf.getBufferSizeInBytes() < (uint64_t) file_size ? buf.getBufferSizeInBytes() : file_size;
  input.read((char *) buf.getBuffer(), size_to_read);
  uint64_t generated_tuples_this_pass = size_to_read / tuple_size;
  buf.setTupleSizeInBytes(tuple_size);
  buf.setNumberOfTuples(generated_tuples_this_pass);

  generatedTuples += generated_tuples_this_pass;
  generatedBuffers++;
}
SourceType BinarySource::getType() const {
    return BINARY_SOURCE;
}
}  // namespace NES
