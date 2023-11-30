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

#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_METRICVALIDATOR_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_METRICVALIDATOR_HPP_

#include <API/Schema.hpp>
#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <Monitoring/Metrics/Gauge/RuntimeMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Metrics/Wrapper/NetworkMetricsWrapper.hpp>
#include <Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/LinuxSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderType.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Util/TestUtils.hpp>
#include <chrono>
#include <nlohmann/json.hpp>

namespace NES {

/**
 * @brief This class is responsible for verifying the content of metrics read by the SystemResourcesReader.
 */
class MetricValidator {
  public:
    static bool isValid(Monitoring::AbstractSystemResourcesReaderPtr reader, Monitoring::MetricPtr metric);

    static bool isValid(Monitoring::AbstractSystemResourcesReaderPtr reader, Monitoring::RuntimeMetrics metrics);

    static bool isValid(Monitoring::AbstractSystemResourcesReaderPtr reader, Monitoring::RegistrationMetrics metrics);

    static bool isValid(Monitoring::AbstractSystemResourcesReaderPtr reader, Monitoring::CpuMetricsWrapper cpuMetrics);

    static bool isValid(Monitoring::AbstractSystemResourcesReaderPtr reader, Monitoring::NetworkMetricsWrapper networkMetrics);

    static bool isValid(Monitoring::AbstractSystemResourcesReaderPtr reader, Monitoring::MemoryMetrics memoryMetrics);

    static bool isValid(Monitoring::AbstractSystemResourcesReaderPtr reader, Monitoring::DiskMetrics diskMetrics);

    static bool isValid(Monitoring::AbstractSystemResourcesReaderPtr reader,
                        Monitoring::StoredNodeMetricsPtr storedMetrics,
                        Monitoring::MetricType expectedType,
                        TopologyNodeId expectedNodeId,
                        uint64_t expectedSize);

    static bool isValidAll(Monitoring::AbstractSystemResourcesReaderPtr reader, nlohmann::json json);

    static bool isValidAllStorage(Monitoring::AbstractSystemResourcesReaderPtr reader, nlohmann::json json);

    static bool isValidRegistrationMetrics(Monitoring::AbstractSystemResourcesReaderPtr reader, nlohmann::json json);

    static bool checkNodeIds(nlohmann::json json, uint64_t nodeId);

    static bool checkNodeIdsStorage(nlohmann::json json, uint64_t nodeId);

    static bool checkEntriesOfStream(std::set<std::string> setOfStr, nlohmann::json jsons);

    static bool checkNodeIds(Monitoring::MetricPtr metric, uint64_t nodeId);

    static bool waitForMonitoringStreamsOrTimeout(const std::set<Monitoring::MetricType>& monitoringStreams,
                                                  uint16_t maxTimeout,
                                                  uint64_t restPort);
};

}// namespace NES

#endif// NES_COORDINATOR_TESTS_INCLUDE_UTIL_METRICVALIDATOR_HPP_
