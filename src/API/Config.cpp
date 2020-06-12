#include <API/Config.hpp>
#include <Util/Logger.hpp>
#include <string>

namespace NES {
Config::Config() {
    numberOfWorker = 1;
    bufferCount = 1;
    bufferSizeInByte = 1024;
}

void Config::print() {
    NES_DEBUG("Config:");
    NES_DEBUG("numberOfWorker=" << numberOfWorker);
    NES_DEBUG("bufferCount=" << bufferCount);
    NES_DEBUG("bufferSizeInByte=" << bufferSizeInByte);
}

Config Config::create() { return Config(); }

Config& Config::setBufferCount(size_t bufferCount) {
    this->bufferCount = bufferCount;
    return *this;
}
size_t Config::getBufferCount() { return bufferCount; }

Config& Config::setBufferSizeInByte(size_t bufferSizeInByte) {
    this->bufferSizeInByte = bufferSizeInByte;
    return *this;
}
size_t Config::getBufferSizeInByte() { return bufferSizeInByte; }

Config& Config::setNumberOfWorker(size_t numberOfWorker) {
    this->numberOfWorker = numberOfWorker;
    return *this;
}
size_t Config::getNumberOfWorker() { return numberOfWorker; }

}// namespace NES
