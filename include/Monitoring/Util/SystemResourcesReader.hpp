#ifndef NES_INCLUDE_RUNTIME_UTIL_SYSTEMRESOURCESCOUNTER_HPP_
#define NES_INCLUDE_RUNTIME_UTIL_SYSTEMRESOURCESCOUNTER_HPP_

#include <Monitoring/Metrics/Gauge.hpp>
#include <string>
#include <unordered_map>

namespace NES {

class SystemResourcesReader {
  public:
    static std::unordered_map<std::string, uint64_t> CPUStats();
    static std::unordered_map<std::string, uint64_t> MemoryStats();
    static std::unordered_map<std::string, uint64_t> DiskStats();
    static std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> NetworkStats();


  private:
    SystemResourcesReader() = default;
};

}

#endif //NES_INCLUDE_RUNTIME_UTIL_SYSTEMRESOURCESCOUNTER_HPP_
