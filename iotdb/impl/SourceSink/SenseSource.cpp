#include <SourceSink/SenseSource.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <assert.h>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <Util/Logger.hpp>
#include <boost/algorithm/string.hpp>
#include <SourceSink/DataSource.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(NES::SenseSource);
using namespace std;

namespace NES {

void SenseSource::setUdsf(std::string udsf)
{
  this->udsf = udsf;
}

SenseSource::SenseSource()
    :
    udsf("") {
}

SenseSource::SenseSource(const Schema& schema , const std::string& udsf)
    :
    DataSource(schema),
    udsf(udsf) {

}

TupleBufferPtr SenseSource::receiveData() {
  NES_DEBUG("SenseSource::receiveData called")
  TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
  fillBuffer(buf);
  NES_DEBUG(
      "SenseSource::receiveData filled buffer with tuples=" << buf->getNumberOfTuples())
  return buf;
}

const std::string SenseSource::toString() const {
  std::stringstream ss;
  ss << "SenseSource(SCHEMA(" << schema.toString() << "), UDSF=" << udsf
     << endl;
  return ss.str();
}

void SenseSource::fillBuffer(TupleBufferPtr buf) {

}

SourceType SenseSource::getType() const {
  return SENSE_SOURCE;
}
}  // namespace NES
