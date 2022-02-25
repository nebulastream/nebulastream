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

#ifndef NES_NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_CPUMETRICSWRAPPER_HPP_
#define NES_NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_CPUMETRICSWRAPPER_HPP_

#include "Monitoring/Metrics/Gauge/CpuMetrics.hpp"
#include "Monitoring/MonitoringForwardRefs.hpp"
#include "Runtime/RuntimeForwardRefs.hpp"

namespace NES {

/**
 * @brief Wrapper class to represent the metrics read from the OS about cpu data.
 */
class CpuMetricsWrapper {
  public:
    CpuMetricsWrapper() = default;
    explicit CpuMetricsWrapper(std::vector<CpuMetrics>&& arr);

    /**
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The object
    */
    void writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const;

    /**
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The object
    */
    void readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex);

    /**
     * @brief Returns the cpu metrics for a given core
     * @param cpuCore core number
     * @return the cpu metrics
    */
    [[nodiscard]] CpuMetrics getValue(unsigned int cpuCore) const;

    /**
     * @brief The number of CPU metrics contained in the wrapper. The number is not equivalent to the number of cores.
     * @return Number of CPU metrics
     */
    [[nodiscard]] uint64_t size() const;

    /**
     * @brief The summarized stats over all CPU metrics. This metric is equivalent to the 0th element of the list.
     * @return CPUMetrics
     */
    [[nodiscard]] CpuMetrics getTotal() const;

    /**
     * @brief Returns the metrics as json
     * @return Json containing the metrics
    */
    web::json::value toJson() const;

    bool operator==(const CpuMetricsWrapper& rhs) const;
    bool operator!=(const CpuMetricsWrapper& rhs) const;

  private:
    std::vector<CpuMetrics> cpuMetrics;
} __attribute__((packed));
/**
 * @brief The serialize method to write CpuMetrics into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the CpuMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
*/
void writeToBuffer(const CpuMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
 * @param schema
 * @param buf
 * @param prefix
 * @return The object
*/
void readFromBuffer(CpuMetricsWrapper& wrapper, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses the metric to JSON
 * @param metrics
 * @return the metrics as JSON
 */
web::json::value asJson(const CpuMetricsWrapper& metrics);

}// namespace NES

#endif//NES_NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_CPUMETRICSWRAPPER_HPP_