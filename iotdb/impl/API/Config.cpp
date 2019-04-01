#include <API/Config.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::Config)

#include <string>

#include <Runtime/DataSink.hpp>

namespace iotdb {
Config::Config()
{
    preloading = false;
    measuring = false;
    numberOfWorker = 1;
    numberOfTuplesPerSource = 1;
    bufferCount = 1;
    bufferSizeInByte = 1024;
}

void Config::print()
{
    std::cout << "Config:" << std::endl;
    std::cout << "preloading=" << preloading << std::endl;
    std::cout << "measuring=" << measuring << std::endl;
    std::cout << "numberOfWorker=" << numberOfWorker << std::endl;
    std::cout << "numberOfTuplesPerSource=" << numberOfTuplesPerSource << std::endl;
    std::cout << "bufferCount=" << bufferCount << std::endl;
    std::cout << "bufferSizeInByte=" << bufferSizeInByte << std::endl;
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


Config& Config::setMeasuring()
{
    this->measuring = true;
    return *this;
}
bool Config::getMeasuring() { return measuring; }

Config& Config::setPreloading(bool value)
{
    this->preloading = value;
    return *this;
}

bool Config::getPreLoading() { return preloading; }

std::string Config::getPreLoadingAsString()
{
    if (preloading)
        return "true";
    else
        return "false";
}

} // namespace iotdb
