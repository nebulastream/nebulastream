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

#ifndef NES_RUNTIME_INCLUDE_MONITORING_METRICS_WRAPPER_NETWORKMETRICSWRAPPER_HPP_
#define NES_RUNTIME_INCLUDE_MONITORING_METRICS_WRAPPER_NETWORKMETRICSWRAPPER_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Monitoring/Metrics/Gauge/NetworkMetrics.hpp>
#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

#include <vector>

namespace NES::Monitoring {

/**
 * @brief Wrapper class to represent a tuple buffer with multiple NetworkMetrics objects.
 */
class NetworkMetricsWrapper {
  public:
    NetworkMetricsWrapper();
    NetworkMetricsWrapper(WorkerId nodeId);
    NetworkMetricsWrapper(std::vector<NetworkMetrics>&& arr);

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
     * @brief Returns the Network metrics for a given core
     * @param NetworkCore core number
     * @return the Network metrics
    */
    [[nodiscard]] NetworkMetrics getNetworkValue(uint64_t interfaceNo) const;

    void addNetworkMetrics(NetworkMetrics&& nwValue);

    [[nodiscard]] uint64_t size() const;

    std::vector<std::string> getInterfaceNames();

    WorkerId getNodeId() const;
    void setNodeId(WorkerId nodeId);

    /**
     * @brief Returns the metrics as json
     * @return Json containing the metrics
    */
    nlohmann::json toJson() const;

    bool operator==(const NetworkMetricsWrapper& rhs) const;
    bool operator!=(const NetworkMetricsWrapper& rhs) const;

  private:
    std::vector<NetworkMetrics> networkMetrics;
    WorkerId nodeId;
    uint64_t timestamp;
} __attribute__((packed));

/**
 * @brief The serialize method to write metrics into the given Schema and TupleBuffer.
 * @param the metrics
 * @param the TupleBuffer
 * @param the tuple index indicating the location of the tuple
*/
void writeToBuffer(const NetworkMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief The deserialize method to read metrics from the given Schema and TupleBuffer.
 * @param the metrics
 * @param the TupleBuffer
 * @param the tuple index indicating the location of the tuple
*/
void readFromBuffer(NetworkMetricsWrapper& wrapper, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses the metric to JSON
 * @param metrics
 * @return the metrics as JSON
 */
nlohmann::json asJson(const NetworkMetricsWrapper& metrics);

}// namespace NES::Monitoring

#endif // NES_RUNTIME_INCLUDE_MONITORING_METRICS_WRAPPER_NETWORKMETRICSWRAPPER_HPP_
