#ifndef NES_INCLUDE_RUNTIME_UTIL_SYSTEMRESOURCESCOUNTER_HPP_
#define NES_INCLUDE_RUNTIME_UTIL_SYSTEMRESOURCESCOUNTER_HPP_

#include <Monitoring/Metrics/Gauge.hpp>
#include <string>
#include <unordered_map>

namespace NES {

class CPU;

/**
 * @brief This is a static utility class to collect basic system information
 * Warning: Only Linux distributions are currently supported
 */
class SystemResourcesReader {
  public:
    /**
     * @brief This method reads CPU information from /proc/stat.
     * @return A map where for each CPU the according /proc/stat information are returned in the form
     * e.g., output["user1"] = 1234, where user is the metric and 1 the cpu core
     */
    static CPU ReadCPUStats();

    /**
     * @brief This method reads memory information from sysinfo
     * @return A map with the memory information
     */
    static std::unordered_map<std::string, uint64_t> MemoryStats();

    /**
     * @brief This method reads disk stats from statvfs
     * @return A map with the disk stats
     */
    static std::unordered_map<std::string, uint64_t> DiskStats();

    /**
     * @brief This methods reads network statistics from /proc/net/dev and returns them for each interface in a
     * separate map
     * @return a map where each interface is mapping the according network statistics map.
     */
    static std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> NetworkStats();


  private:
    SystemResourcesReader() = default;
};

}

#endif //NES_INCLUDE_RUNTIME_UTIL_SYSTEMRESOURCESCOUNTER_HPP_
