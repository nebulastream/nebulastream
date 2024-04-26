/*
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

#ifndef NES_RUNTIME_INCLUDE_MONITORING_METRICS_WRAPPER_CPUMETRICSWRAPPER_HPP_
#define NES_RUNTIME_INCLUDE_MONITORING_METRICS_WRAPPER_CPUMETRICSWRAPPER_HPP_

#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/TopologyLinkInformation.hpp>

namespace NES::Monitoring {

/**
 * @brief Wrapper class to represent a tuple buffer with multiple CpuMetrics objects.
 */
class CpuMetricsWrapper {
  public:
    /**
     * Default Ctor for CpuMetricsWrapper with timestamp=0.
     */
    CpuMetricsWrapper();
    /**
     * Ctor for CpuMetricsWrapper with a nodeId.
     * @param nodeId
     */
    explicit CpuMetricsWrapper(uint64_t nodeId);
    /**
     * Ctor for CpuMetricsWrapper with a set of CpuMetrics. Node is 0 here.
     * @param arr CpuMetrics
     */
    explicit CpuMetricsWrapper(std::vector<CpuMetrics>&& arr);

    /**
     * @brief Writes a wrapper object to a given TupleBuffer.
     * @param buf
     * @param tupleIndex
    */
    void writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const;

    /**
     * @brief Parses a wrapper object from a given TupleBuffer.
     * @param buf
     * @param tupleIndex
    */
    void readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex);

    /**
     * @brief Returns the cpu metrics for a given core
     * @param cpuCore core number
     * @return the cpu metrics
    */
    [[nodiscard]] CpuMetrics getValue(unsigned int cpuCore) const;

    /**
     * @brief The number of CPU metrics contained in the wrapper. The number is equivalent to the number of cores + 1.
     * @return Number of CPU metrics
     */
    [[nodiscard]] uint64_t size() const;

    /**
     * @brief The summarized stats over all CPU metrics. This metric is equivalent to the 0th element of the list.
     * @return CPUMetrics
     */
    [[nodiscard]] CpuMetrics getTotal() const;

    WorkerId getNodeId() const;
    void setNodeId(WorkerId nodeId);

    /**
     * @brief Returns the metrics as json
     * @return Json containing the metrics
    */
    nlohmann::json toJson() const;

    bool operator==(const CpuMetricsWrapper& rhs) const;
    bool operator!=(const CpuMetricsWrapper& rhs) const;

  private:
    std::vector<CpuMetrics> cpuMetrics;
    WorkerId nodeId;
    uint64_t timestamp;
} __attribute__((packed));

/**
 * @brief The serialize method to write metrics into the given Schema and TupleBuffer.
 * @param the metrics
 * @param the TupleBuffer
 * @param the tuple index indicating the location of the tuple
*/
void writeToBuffer(const CpuMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief The deserialize method to read metrics from the given Schema and TupleBuffer.
 * @param the metrics
 * @param the TupleBuffer
 * @param the tuple index indicating the location of the tuple
*/
void readFromBuffer(CpuMetricsWrapper& wrapper, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses the metric to JSON
 * @param metrics
 * @return the metrics as JSON
 */
nlohmann::json asJson(const CpuMetricsWrapper& metrics);

}// namespace NES::Monitoring

#endif // NES_RUNTIME_INCLUDE_MONITORING_METRICS_WRAPPER_CPUMETRICSWRAPPER_HPP_
