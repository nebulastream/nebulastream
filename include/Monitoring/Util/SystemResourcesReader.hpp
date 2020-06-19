#ifndef NES_INCLUDE_RUNTIME_UTIL_SYSTEMRESOURCESCOUNTER_HPP_
#define NES_INCLUDE_RUNTIME_UTIL_SYSTEMRESOURCESCOUNTER_HPP_

#include <chrono>

class SystemResourcesReader {
  public:
    SystemResourcesReader();

  private:
    std::chrono::milliseconds probeInterval;

};


#endif //NES_INCLUDE_RUNTIME_UTIL_SYSTEMRESOURCESCOUNTER_HPP_
