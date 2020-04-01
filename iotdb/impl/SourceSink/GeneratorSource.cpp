#include <boost/serialization/export.hpp>
#include <SourceSink/GeneratorSource.hpp>
BOOST_CLASS_EXPORT(NES::GeneratorSource);
#include <NodeEngine/BufferManager.hpp>

#include <iostream>

namespace NES {

const std::string GeneratorSource::toString() const {
  std::stringstream ss;
  ss << "GENERATOR_SOURCE(SCHEMA(" << schema->toString();
  ss << "), NUM_BUFFERS=" << this->numBuffersToProcess << " frequency=" << this->gatheringInterval << "))";
  return ss.str();
}
SourceType GeneratorSource::getType() const {
    return TEST_SOURCE;
}
}

