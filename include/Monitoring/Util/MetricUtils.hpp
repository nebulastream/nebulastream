#ifndef NES_INCLUDE_MONITORING_METRICS_METRICUTILS_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRICUTILS_HPP_

#include <unordered_map>
#include <Monitoring/Metrics/Gauge.hpp>

namespace NES {
class CPU;

/**
 * @brief Pre-defined metrics used for NES internally.
 */
class MetricUtils {
  public:
    /**
     * @brief Gauge metric for the CPU stats represented as map
     * @return the cpu stats
     */
    static Gauge<CPU> CPUStats();

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
    static Gauge<uint64_t> CPUIdle(unsigned int cpuNo);

  private:
    MetricUtils() = default;

};

}

#endif //NES_INCLUDE_MONITORING_METRICS_METRICUTILS_HPP_
