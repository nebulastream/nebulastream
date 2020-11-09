/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

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
