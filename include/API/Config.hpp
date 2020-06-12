#pragma once

#include <string>

namespace NES {

class Config {
  public:
    static Config create();

    Config& setBufferCount(size_t bufferCount);
    size_t getBufferCount();

    Config& setBufferSizeInByte(size_t bufferSizeInByte);
    size_t getBufferSizeInByte();

    Config& setNumberOfWorker(size_t numberOfWorker);
    size_t getNumberOfWorker();

    void print();
    Config();

  private:
    size_t numberOfWorker;
    size_t bufferCount;
    size_t bufferSizeInByte;
};
}// namespace NES
