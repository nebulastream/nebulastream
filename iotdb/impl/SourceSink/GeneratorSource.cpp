#include <SourceSink/GeneratorSource.hpp>

#include <NodeEngine/BufferManager.hpp>

#include <iostream>

namespace iotdb {

const std::string GeneratorSource::toString() const {
  std::stringstream ss;
  ss << "GENERATOR_SOURCE(SCHEMA(" << schema.toString();
  ss << "), NUM_BUFFERS=" << num_buffers_to_process << "))";
  return ss.str();
}
}

BOOST_CLASS_EXPORT(iotdb::GeneratorSource);