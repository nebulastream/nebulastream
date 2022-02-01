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

#ifndef NES_INCLUDE_MONITORING_UTIL_METRICUTILS_HPP_
#define NES_INCLUDE_MONITORING_UTIL_METRICUTILS_HPP_

#include <Monitoring/Metrics/Gauge.hpp>
#include <Monitoring/Util/AbstractSystemResourcesReader.hpp>
#include <unordered_map>

namespace NES {

class CpuMetrics;
class MemoryMetrics;
class DiskMetrics;
class NetworkMetrics;
class RuntimeNesMetrics;
class StaticNesMetrics;
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

/**
 * @brief Pre-defined metrics used for NES internally.
 */
class MetricUtils {
  public:
    /**
     * @brief Creates the appropriate SystemResourcesReader for the OS
     * @return the SystemResourcesReader
     */
    static std::unique_ptr<AbstractSystemResourcesReader> getSystemResourcesReader();

    /**
     * @brief Gauge metric for reading the runtime stats of NES
     * @return the cpu stats
     */
    static Gauge<RuntimeNesMetrics> runtimeNesStats();

    /**
     * @brief Gauge metric for reading the static stats of NES
     * @return the cpu stats
     */
    static Gauge<StaticNesMetrics> staticNesStats();

    /**
     * @brief Gauge metric for reading the CPU stats
     * @return the cpu stats
     */
    static Gauge<CpuMetrics> cpuStats();

    /**
     * @brief Gauge metric for reading the memory stats
     * @return the memory stats
     */
    static Gauge<MemoryMetrics> memoryStats();

    /**
     * @brief Gauge metric for reading the disk stats
     * @return the disk stats
     */
    static Gauge<DiskMetrics> diskStats();

    /**
     * @brief Gauge metric for reading reading the network stats
     * @return the network stats
     */
    static Gauge<NetworkMetrics> networkStats();

    /**
     * @brief Gauge metric for reading idle of the cpu
     * @param the cpu core
     * @return the gauge metric
     */
    static Gauge<uint64_t> cpuIdle(unsigned int cpuNo);

    /**
     *
     * @param metricSchema
     * @param bufferSchema
     * @param prefix
     * @return
     */
    static bool validateFieldsInSchema(SchemaPtr metricSchema, SchemaPtr bufferSchema, uint64_t schemaIndex);

  private:
    MetricUtils() = default;
};

}// namespace NES

#endif  // NES_INCLUDE_MONITORING_UTIL_METRICUTILS_HPP_
