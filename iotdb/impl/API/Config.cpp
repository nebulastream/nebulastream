#include <API/Config.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::Config)

#include <string>
#include <Util/Logger.hpp>
#include <Runtime/DataSink.hpp>

namespace iotdb {
Config::Config()
{
    numberOfWorker = 1;
    bufferCount = 1;
    bufferSizeInByte = 1024;
}

void Config::print()
{
    IOTDB_DEBUG("Config:")
    IOTDB_DEBUG("numberOfWorker=" << numberOfWorker)
    IOTDB_DEBUG("bufferCount=" << bufferCount)
    IOTDB_DEBUG("bufferSizeInByte=" << bufferSizeInByte)
}

Config Config::create() { return Config(); }

Config& Config::setBufferCount(size_t bufferCount)
{
    this->bufferCount = bufferCount;
    return *this;
}
size_t Config::getBufferCount() { return bufferCount; }


Config& Config::setBufferSizeInByte(size_t bufferSizeInByte)
{
    this->bufferSizeInByte = bufferSizeInByte;
    return *this;
}
size_t Config::getBufferSizeInByte() { return bufferSizeInByte; }


Config& Config::setNumberOfWorker(size_t numberOfWorker)
{
    this->numberOfWorker = numberOfWorker;
    return *this;
}
size_t Config::getNumberOfWorker() { return numberOfWorker; }


} // namespace iotdb
