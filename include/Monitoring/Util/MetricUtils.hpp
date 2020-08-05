#ifndef NES_INCLUDE_MONITORING_METRICS_METRICUTILS_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRICUTILS_HPP_

#include <Monitoring/Metrics/Gauge.hpp>
#include <unordered_map>

namespace NES {

class CpuMetrics;
class MemoryMetrics;
class DiskMetrics;
class NetworkMetrics;

/**
 * @brief Pre-defined metrics used for NES internally.
 */
class MetricUtils {
  public:
    /**
     * @brief Gauge metric for reading the CPU stats
     * @return the cpu stats
     */
    static Gauge<CpuMetrics> CPUStats();

    /**
     * @brief Gauge metric for reading the memory stats
     * @return the memory stats
     */
    static Gauge<MemoryMetrics> MemoryStats();

    /**
     * @brief Gauge metric for reading the disk stats
     * @return the disk stats
     */
    static Gauge<DiskMetrics> DiskStats();

    /**
     * @brief Gauge metric for reading reading the network stats
     * @return the network stats
     */
    static Gauge<NetworkMetrics> NetworkStats();

    /**
     * @brief Gauge metric for reading idle of the cpu
     * @param the cpu core
     * @return the gauge metric
     */
    static Gauge<uint64_t> CPUIdle(unsigned int cpuNo);

  private:
    MetricUtils() = default;
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICS_METRICUTILS_HPP_
