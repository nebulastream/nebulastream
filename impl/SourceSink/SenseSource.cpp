#include <NodeEngine/QueryManager.hpp>
#include <SourceSink/DataSource.hpp>
#include <SourceSink/SenseSource.hpp>
#include <Util/Logger.hpp>
#include <assert.h>
#include <boost/algorithm/string.hpp>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
BOOST_CLASS_EXPORT_IMPLEMENT(NES::SenseSource);
using namespace std;

namespace NES {

SenseSource::SenseSource()
    : udsf("") {
}

SenseSource::SenseSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& udsf)
    : DataSource(schema, bufferManager, queryManager),
      udsf(udsf) {
}

std::optional<TupleBuffer> SenseSource::receiveData() {
    NES_DEBUG("SenseSource::receiveData called");
    auto buf = bufferManager->getBufferBlocking();
    fillBuffer(buf);
    NES_DEBUG("SenseSource::receiveData filled buffer with tuples=" << buf.getNumberOfTuples());
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

const string& SenseSource::getUdsf() const {
    return udsf;
}

}// namespace NES
