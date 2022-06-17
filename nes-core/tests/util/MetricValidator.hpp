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

#ifndef NES_TESTS_UTIL_METRICVALIDATOR_HPP_
#define NES_TESTS_UTIL_METRICVALIDATOR_HPP_

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
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <chrono>
#include <cpprest/json.h>

namespace NES {

/**
 * @brief This class is responsible for verifying the content of metrics read by the SystemResourcesReader.
 */
class MetricValidator {
  public:
    static bool isValid(AbstractSystemResourcesReaderPtr reader, MetricPtr metric) {
        if (metric->getMetricType() == DiskMetric) {
            return isValid(reader, metric->getValue<DiskMetrics>());
        } else if (metric->getMetricType() == MemoryMetric) {
            return isValid(reader, metric->getValue<MemoryMetrics>());
        } else if (metric->getMetricType() == RegistrationMetric) {
            return isValid(reader, metric->getValue<RegistrationMetrics>());
        } else if (metric->getMetricType() == WrappedCpuMetrics) {
            return isValid(reader, metric->getValue<CpuMetricsWrapper>());
        } else if (metric->getMetricType() == WrappedNetworkMetrics) {
            return isValid(reader, metric->getValue<NetworkMetricsWrapper>());
        } else {
            return false;
        }
    };

    static bool isValid(AbstractSystemResourcesReaderPtr reader, RuntimeMetrics metrics) {
        bool check = true;

        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used for RuntimeMetrics. Returning true");
            return true;
        }

        if (metrics.wallTimeNs <= 0) {
            NES_ERROR("MetricValidator: Wrong wallTimeNs.");
            check = false;
        }
        if (!(metrics.blkioBytesWritten >= 0)) {
            NES_ERROR("MetricValidator: Wrong blkioBytesWritten.");
            check = false;
        }
        if (!(metrics.blkioBytesRead >= 0)) {
            NES_ERROR("MetricValidator: Wrong blkioBytesRead.");
            check = false;
        }
        if (!(metrics.memoryUsageInBytes >= 0)) {
            NES_ERROR("MetricValidator: Wrong memoryUsageInBytes.");
            check = false;
        }
        if (!(metrics.cpuLoadInJiffies >= 0)) {
            NES_ERROR("MetricValidator: Wrong cpuLoadInJiffies.");
            check = false;
        }
        if (!(metrics.longCoord >= 0)) {
            NES_ERROR("MetricValidator: Wrong longCoord.");
            check = false;
        }
        if (!(metrics.latCoord >= 0)) {
            NES_ERROR("MetricValidator: Wrong latCoord.");
            check = false;
        }
        if (!(metrics.batteryStatusInPercent >= 0)) {
            NES_ERROR("MetricValidator: Wrong batteryStatusInPercent.");
            check = false;
        }
        return check;
    };

    static bool isValid(AbstractSystemResourcesReaderPtr reader, RegistrationMetrics metrics) {
        bool check = true;

        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used for NodeRegistrationMetrics. Returning true");
            return true;
        }

        if (!(metrics.cpuQuotaUS >= -1)) {
            NES_ERROR("MetricValidator: Wrong cpuQuotaUS.");
            check = false;
        }
        if (!(metrics.cpuPeriodUS >= -1)) {
            NES_ERROR("MetricValidator: Wrong cpuPeriodUS.");
            check = false;
        }
        if (!(metrics.totalCPUJiffies > 0)) {
            NES_ERROR("MetricValidator: Wrong totalCPUJiffies.");
            check = false;
        }
        if (!(metrics.cpuCoreNum > 0)) {
            NES_ERROR("MetricValidator: Wrong cpuCoreNum.");
            check = false;
        }
        if (!(metrics.totalMemoryBytes >= 0)) {
            NES_ERROR("MetricValidator: Wrong totalMemoryBytes.");
            check = false;
        }
        if (metrics.hasBattery) {
            NES_ERROR("MetricValidator: Wrong hasBattery.");
            check = false;
        }
        if (metrics.isMoving) {
            NES_ERROR("MetricValidator: Wrong isMoving.");
            check = false;
        }
        return check;
    };

    static bool isValid(AbstractSystemResourcesReaderPtr reader, CpuMetricsWrapper cpuMetrics) {
        bool check = true;

        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used for CpuMetricsWrapper. Returning true");
            return true;
        }

        if (cpuMetrics.size() <= 0) {
            NES_ERROR("MetricValidator: Wrong getNumCores.");
            check = false;
        }
        for (uint64_t i = 0; i < cpuMetrics.size(); i++) {
            if (!(cpuMetrics.getValue(i).user > 0)) {
                NES_ERROR("MetricValidator: Wrong cpuMetrics.getValue(i).user.");
                check = false;
            }
        }
        if (!(cpuMetrics.getTotal().user > 0)) {
            NES_ERROR("MetricValidator: Wrong cpuMetrics.getTotal().user.");
            check = false;
        }
        return check;
    };

    static bool isValid(AbstractSystemResourcesReaderPtr reader, NetworkMetricsWrapper networkMetrics) {
        bool check = true;

        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used for NetworkMetricsWrapper. Returning true");
            return true;
        }

        if (networkMetrics.getInterfaceNames().empty()) {
            NES_ERROR("MetricValidator: Wrong getInterfaceNames().");
            check = false;
        }
        return check;
    };

    static bool isValid(AbstractSystemResourcesReaderPtr reader, MemoryMetrics memoryMetrics) {
        bool check = true;

        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used for MemoryMetrics. Returning true");
            return true;
        }

        if (!(memoryMetrics.FREE_RAM > 0)) {
            NES_ERROR("MetricValidator: Wrong FREE_RAM.");
            check = false;
        }
        return check;
    };

    static bool isValid(AbstractSystemResourcesReaderPtr reader, DiskMetrics diskMetrics) {
        bool check = true;

        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used for DiskMetrics. Returning true");
            return true;
        }

        if (!(diskMetrics.fBavail >= 0)) {
            NES_ERROR("MetricValidator: Wrong fBavail.");
            check = false;
        }
        return check;
    }

    static bool isValid(AbstractSystemResourcesReaderPtr reader,
                        StoredNodeMetricsPtr storedMetrics,
                        MetricType expectedType,
                        TopologyNodeId expectedNodeId,
                        uint64_t expectedSize) {
        bool check = true;

        if (!storedMetrics->contains(expectedType)) {
            NES_ERROR("MetricValidator: Metrics for node " << expectedNodeId << " are missing type " << toString(expectedType));
            return false;
        }

        auto metricVec = storedMetrics->at(expectedType);
        TimestampMetricPtr pairedNetworkMetric = metricVec->at(0);
        MetricPtr retMetric = pairedNetworkMetric->second;

        NES_INFO("MetricValidator: Stored metrics for ID " << expectedNodeId << ": " << MetricUtils::toJson(storedMetrics));
        if (retMetric->getMetricType() != expectedType) {
            NES_ERROR("MetricValidator: MetricType is not as expected " << toString(retMetric->getMetricType())
                                                                        << " != " << toString(expectedType));
            check = false;
        }

        if (!MetricValidator::isValid(reader, retMetric)) {
            check = false;
        }

        if (!MetricValidator::checkNodeIds(retMetric, expectedNodeId)) {
            check = false;
        }

        if (storedMetrics->size() != expectedSize) {
            NES_ERROR("MetricValidator: Stored metrics do not match expected size " << storedMetrics->size()
                                                                                    << " != " << expectedSize);
            check = false;
        }
        return check;
    }

    static bool isValidAll(AbstractSystemResourcesReaderPtr reader, web::json::value json) {
        bool check = true;

        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used. Returning true");
            return true;
        }

        if (!json.has_field("registration")) {
            NES_ERROR("MetricValidator: Missing field registration");
            check = false;
        } else {
            check = isValidRegistrationMetrics(reader, json["registration"]);
        }

        if (!json.has_field("disk")) {
            NES_ERROR("MetricValidator: Missing field disk");
            check = false;
        } else {
            if (!(json["disk"].size() == 6U)) {
                NES_ERROR("MetricValidator: Values for disk missing");
                check = false;
            }
        }

        if (!json.has_field("wrapped_cpu")) {
            NES_ERROR("MetricValidator: Missing field wrapped cpu");
            check = false;
        } else {
            auto numCpuFields = json["wrapped_cpu"].size();
            if (numCpuFields <= 1) {
                NES_ERROR("MetricValidator: Values for wrapped_cpu missing");
                check = false;
            }
        }

        if (!json.has_field("wrapped_network")) {
            NES_ERROR("MetricValidator: Missing field wrapped network");
            check = false;
        } else {
            auto numFields = json["wrapped_network"].size();
            if (numFields < 1) {
                NES_ERROR("MetricValidator: Values for wrapped_network missing");
                check = false;
            }
        }

        if (!json.has_field("memory")) {
            NES_ERROR("MetricValidator: Missing field memory");
            check = false;
        } else {
            auto numFields = json["memory"].size();
            if (numFields < 13) {
                NES_ERROR("MetricValidator: Values for wrapped_network missing");
                check = false;
            }
        }
        return check;
    }

    static bool isValidAllStorage(AbstractSystemResourcesReaderPtr reader, web::json::value json) {
        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used. Returning true");
            return true;
        }

        bool check = true;
        if (!json.has_field("registration")) {
            NES_ERROR("MetricValidator: Missing field registration");
            check = false;
        } else {
            check = isValidRegistrationMetrics(reader, json["registration"][0]["value"]);
        }

        if (!json.has_field("disk")) {
            NES_ERROR("MetricValidator: Missing field disk");
            check = false;
        } else {
            if (!(json["disk"][0]["value"].size() == 6U)) {
                NES_ERROR("MetricValidator: Values for disk missing");
                check = false;
            }
        }

        if (!json.has_field("wrapped_cpu")) {
            NES_ERROR("MetricValidator: Missing field wrapped cpu");
            check = false;
        } else {
            auto numCpuFields = json["wrapped_cpu"][0]["value"].size();
            if (numCpuFields <= 1) {
                NES_ERROR("MetricValidator: Values for wrapped_cpu missing");
                check = false;
            }
        }

        if (!json.has_field("wrapped_network")) {
            NES_ERROR("MetricValidator: Missing field wrapped network");
            check = false;
        } else {
            auto numFields = json["wrapped_network"][0]["value"].size();
            if (numFields < 1) {
                NES_ERROR("MetricValidator: Values for wrapped_network missing");
                check = false;
            }
        }

        if (!json.has_field("memory")) {
            NES_ERROR("MetricValidator: Missing field memory");
            check = false;
        } else {
            auto numFields = json["memory"][0]["value"].size();
            if (numFields < 13) {
                NES_ERROR("MetricValidator: Values for wrapped_network missing");
                check = false;
            }
        }
        return check;
    }

    static bool isValidRegistrationMetrics(AbstractSystemResourcesReaderPtr reader, web::json::value json) {
        bool check = true;

        if (reader->getReaderType() == SystemResourcesReaderType::AbstractReader) {
            NES_WARNING("MetricValidator: AbstractReader used for DiskMetrics.");
            auto numFields = json.size();
            if (numFields != RegistrationMetrics::getSchema("")->getSize()) {
                NES_ERROR("MetricValidator: Entries for registration metrics missing");
                return false;
            }
            return true;
        }

        if (!(json.has_field("CpuCoreNum"))) {
            NES_ERROR("MetricValidator: Wrong CpuCoreNum.");
            check = false;
        }

        if (!(json.has_field("CpuPeriodUS"))) {
            NES_ERROR("MetricValidator: Wrong CpuPeriodUS.");
            check = false;
        }

        if (!(json.has_field("CpuQuotaUS"))) {
            NES_ERROR("MetricValidator: Wrong CpuQuotaUS.");
            check = false;
        }

        if (!(json.has_field("HasBattery"))) {
            NES_ERROR("MetricValidator: Wrong HasBattery.");
            check = false;
        }

        if (!(json.has_field("IsMoving"))) {
            NES_ERROR("MetricValidator: Wrong IsMoving.");
            check = false;
        }

        if (!(json.has_field("TotalCPUJiffies"))) {
            NES_ERROR("MetricValidator: Wrong TotalCPUJiffies.");
            check = false;
        }

        if (!(json.has_field("TotalMemory"))) {
            NES_ERROR("MetricValidator: Wrong TotalMemory.");
            check = false;
        }

        return check;
    }

    static bool checkNodeIds(web::json::value json, uint64_t nodeId) {
        bool check = true;
        for (auto iter = json.as_object().cbegin(); iter != json.as_object().cend(); ++iter) {
            // This change lets you get the string straight up from "first"
            const utility::string_t& str = iter->first;
            const web::json::value& v = iter->second;
            if (json[str].has_field("NODE_ID") && json[str]["NODE_ID"] != nodeId) {
                NES_ERROR("MetricValidator: Wrong node ID for " << str << " where " << json[str]["NODE_ID"] << "!=" << nodeId);
                check = false;
            }
        }
        return check;
    }

    static bool checkNodeIdsStorage(web::json::value json, uint64_t nodeId) {
        bool check = true;
        for (auto iter = json.as_object().cbegin(); iter != json.as_object().cend(); ++iter) {
            // This change lets you get the string straight up from "first"
            const utility::string_t& str = iter->first;
            auto jsonMetric = json[str][0]["value"];
            if (!checkNodeIds(jsonMetric, nodeId)) {
                check = false;
            }
        }
        return check;
    }

    static bool checkNodeIds(MetricPtr metric, uint64_t nodeId) {
        if (metric->getMetricType() == DiskMetric) {
            auto parsedMetrics = metric->getValue<DiskMetrics>();
            return parsedMetrics.nodeId == nodeId;
        } else if (metric->getMetricType() == MemoryMetric) {
            auto parsedMetrics = metric->getValue<MemoryMetrics>();
            return parsedMetrics.nodeId == nodeId;
        } else if (metric->getMetricType() == RegistrationMetric) {
            auto parsedMetrics = metric->getValue<RegistrationMetrics>();
            return parsedMetrics.nodeId == nodeId;
        } else if (metric->getMetricType() == WrappedCpuMetrics) {
            auto parsedMetrics = metric->getValue<CpuMetricsWrapper>();
            for (uint64_t i = 0; i < parsedMetrics.size(); i++) {
                if (parsedMetrics.getValue(i).nodeId != nodeId) {
                    return false;
                }
            }
            return parsedMetrics.getNodeId() == nodeId;
        } else if (metric->getMetricType() == WrappedNetworkMetrics) {
            auto parsedMetrics = metric->getValue<NetworkMetricsWrapper>();
            for (uint64_t i = 0; i < parsedMetrics.size(); i++) {
                if (parsedMetrics.getNetworkValue(i).nodeId != nodeId) {
                    return false;
                }
            }
            return parsedMetrics.getNodeId() == nodeId;
        } else {
            return false;
        }
    }

    static bool
    waitForMonitoringStreamsOrTimeout(std::set<std::string> monitoringStreams, uint16_t maxTimeout, uint64_t restPort) {
        for (int i = 0; i < maxTimeout; i++) {
            auto json = TestUtils::makeMonitoringRestCall("storage", std::to_string(restPort));
            NES_INFO("MetricValidator: Testing metric call " << json);
            bool missing = false;
            for (uint64_t nodeId = 1; nodeId <= json.size(); nodeId++) {
                for (auto stream : monitoringStreams) {
                    if (!json[std::to_string(nodeId)].has_field(stream)) {
                        NES_ERROR("MetricValidator: Missing metric " << stream << " for node " << nodeId);
                        missing = true;
                        break;
                    }
                }
                if (missing) {
                    break;
                }
            }

            if (missing) {
                std::this_thread::sleep_for(std::chrono::seconds(2));
            } else {
                NES_INFO("MetricValidator: All metrics available");
                return true;
            }
        }
        NES_ERROR("MetricValidator: Timeout passed");
        return false;
    }
};

}// namespace NES

#endif//NES_TESTS_UTIL_METRICVALIDATOR_HPP_