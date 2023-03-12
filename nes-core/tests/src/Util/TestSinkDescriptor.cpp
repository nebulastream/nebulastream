#include <Util/TestSinkDescriptor.hpp>

namespace NES::TestUtils {

TestSinkDescriptor::TestSinkDescriptor(DataSinkPtr dataSink) : sink(std::move(std::move(dataSink))) {}

DataSinkPtr TestSinkDescriptor::getSink() { return sink; }

std::string TestSinkDescriptor::toString() { return std::string(); }

bool TestSinkDescriptor::equal(const SinkDescriptorPtr&) { return false; }

}// namespace NES::TestUtils