#ifndef NES_INCLUDE_MONITORING_METRICS_NESMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICS_NESMETRICS_HPP_

#include <unordered_map>
#include <Monitoring/Metrics/Gauge.hpp>

namespace NES {

/**
 * @brief NesMetrics are pre-defined metrics used for NES internally.
 */
class NesMetrics {
  public:
    /**
     * @brief Gauge metric for the CPU stats represented as map
     * @return the cpu stats
     */
    static Gauge<std::unordered_map<std::string, uint64_t>> CPUStats();

    /**
     * @brief Gauge metric for the memory stats represented as map
     * @return the memory stats
     */
    static Gauge<std::unordered_map<std::string, uint64_t>> MemoryStats();

    /**
     * @brief Gauge metric for the disk stats represented as map
     * @return the disk stats
     */
    static Gauge<std::unordered_map<std::string, uint64_t>> DiskStats();

    /**
     * @brief Gauge metric for the network stats represented as map
     * @return the network stats
     */
    static Gauge<std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>>> NetworkStats();

    /**
     * @brief
     * @param cpuName
     * @return
     */
    static Gauge<uint64_t> CPUIdle(std::string cpuName);

  private:
    NesMetrics() = default;

};

}

#endif //NES_INCLUDE_MONITORING_METRICS_NESMETRICS_HPP_
