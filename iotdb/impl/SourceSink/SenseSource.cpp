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

void SenseSource::setUdsf(std::string udsf) {
    this->udsf = udsf;
}

SenseSource::SenseSource()
    :
    udsf("") {
}

SenseSource::SenseSource(SchemaPtr schema , const std::string& udsf)
    :
    DataSource(schema),
    udsf(udsf) {

}

std::optional<TupleBuffer> SenseSource::receiveData(DispatcherPtr dispatcher) {
    NES_DEBUG("SenseSource::receiveData called")
    auto buf = dispatcher->getBufferManager()->getBufferBlocking();
    fillBuffer(buf);
    NES_DEBUG(
        "SenseSource::receiveData filled buffer with tuples=" << buf.getNumberOfTuples())
    return buf;
}

const std::string SenseSource::toString() const {
  std::stringstream ss;
  ss << "SenseSource(SCHEMA(" << schema->toString() << "), UDSF=" << udsf
     << endl;
  return ss.str();
}

void SenseSource::fillBuffer(TupleBuffer& buf) {

}

SourceType SenseSource::getType() const {
    return SENSE_SOURCE;
}
}  // namespace NES
