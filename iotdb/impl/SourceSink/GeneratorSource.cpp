#include <SourceSink/GeneratorSource.hpp>

#include <NodeEngine/BufferManager.hpp>

#include <iostream>

namespace NES {

const std::string GeneratorSource::toString() const {
  std::stringstream ss;
  ss << "GENERATOR_SOURCE(SCHEMA(" << schema.toString();
  ss << "), NUM_BUFFERS=" << numBuffersToProcess << "))";
  return ss.str();
}
SourceType GeneratorSource::getType() const {
    return TEST_SOURCE;
}
}

BOOST_CLASS_EXPORT(NES::GeneratorSource);