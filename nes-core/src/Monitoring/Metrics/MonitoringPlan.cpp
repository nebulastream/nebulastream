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

#include <Monitoring/Metrics/MonitoringPlan.hpp>

#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/DiskMetrics.hpp>
#include <Monitoring/MetricValues/GroupedMetricValues.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/Metrics/Gauge.hpp>
#include <Monitoring/Metrics/MetricCatalog.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Util/Logger.hpp>

#include <WorkerRPCService.pb.h>

namespace NES {

const std::string MonitoringPlan::CPU_METRICS_DESC = "cpuMetrics_";
const std::string MonitoringPlan::CPU_VALUES_DESC = "cpuValues_";
const std::string MonitoringPlan::NETWORK_METRICS_DESC = "networkMetrics_";
const std::string MonitoringPlan::NETWORK_VALUES_DESC = "networkValues_";
const std::string MonitoringPlan::MEMORY_METRICS_DESC = "memoryMetrics_";
const std::string MonitoringPlan::DISK_METRICS_DESC = "diskMetrics_";

MonitoringPlan::MonitoringPlan(const std::vector<MetricValueType>& metrics)
    : cpuMetrics(false), networkMetrics(false), memoryMetrics(false), diskMetrics(false) {
    NES_DEBUG("MonitoringPlan: Initializing");
    addMetrics(metrics);
}

MonitoringPlan::MonitoringPlan(const SerializableMonitoringPlan& plan)
    : cpuMetrics(plan.cpumetrics()), networkMetrics(plan.networkmetrics()), memoryMetrics(plan.memorymetrics()),
      diskMetrics(plan.diskmetrics()) {
    NES_DEBUG("MonitoringPlan: Initializing from shippable protobuf object.");
}

MonitoringPlanPtr MonitoringPlan::DefaultPlan() {
    return MonitoringPlan::create(std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric}));
}

MonitoringPlanPtr MonitoringPlan::create(const std::vector<MetricValueType>& metrics) {
    return std::make_shared<MonitoringPlan>(MonitoringPlan(metrics));
}

MonitoringPlanPtr MonitoringPlan::create(const SerializableMonitoringPlan& shippable) {
    return std::make_shared<MonitoringPlan>(MonitoringPlan(shippable));
}

bool MonitoringPlan::addMetric(MetricValueType metric) {
    switch (metric) {
        case CpuMetric: {
            if (cpuMetrics) {
                NES_INFO("MonitoringPlan: Metric already set for CpuMetric.");
                return false;
            }
            cpuMetrics = true;
            return true;
        };
        case DiskMetric: {
            if (diskMetrics) {
                NES_INFO("MonitoringPlan: Metric already set for DiskMetric.");
                return false;
            }
            diskMetrics = true;
            return true;
        };
        case MemoryMetric: {
            if (memoryMetrics) {
                NES_INFO("MonitoringPlan: Metric already set for MemoryMetric.");
                return false;
            }
            memoryMetrics = true;
            return true;
        };
        case NetworkMetric: {
            if (networkMetrics) {
                NES_INFO("MonitoringPlan: Metric already set for NetworkMetric.");
                return false;
            }
            networkMetrics = true;
            return true;
        };
        default: {
            NES_INFO("MonitoringPlan: Metric not defined.");
            return false;
        }
    }
}

void MonitoringPlan::addMetrics(const std::vector<MetricValueType>& metrics) {
    for (const auto& m : metrics) {
        addMetric(m);
    }
}

MetricGroupPtr MonitoringPlan::createMetricGroup(const MetricCatalogPtr&) const {
    MetricGroupPtr metricGroup = MetricGroup::create();

    if (cpuMetrics) {
        NES_DEBUG("MonitoringPlan: Adding " + MonitoringPlan::CPU_METRICS_DESC + " to monitoring plan.");
        Gauge<CpuMetrics> cpuStats = MetricUtils::cpuStats();
        metricGroup->add(MonitoringPlan::CPU_METRICS_DESC, NES::Metric{cpuStats});
    }
    if (diskMetrics) {
        NES_DEBUG("MonitoringPlan: Adding " + MonitoringPlan::DISK_METRICS_DESC + " to monitoring plan.");
        Gauge<DiskMetrics> diskStats = MetricUtils::diskStats();
        metricGroup->add(MonitoringPlan::DISK_METRICS_DESC, NES::Metric{diskStats});
    }
    if (memoryMetrics) {
        NES_DEBUG("MonitoringPlan: Adding " + MonitoringPlan::MEMORY_METRICS_DESC + " to monitoring plan.");
        Gauge<MemoryMetrics> memStats = MetricUtils::memoryStats();
        metricGroup->add(MonitoringPlan::MEMORY_METRICS_DESC, NES::Metric{memStats});
    }
    if (networkMetrics) {
        NES_DEBUG("MonitoringPlan: Adding " + MonitoringPlan::NETWORK_METRICS_DESC + " to monitoring plan.");
        Gauge<NetworkMetrics> networkStats = MetricUtils::networkStats();
        metricGroup->add(MonitoringPlan::NETWORK_METRICS_DESC, NES::Metric{networkStats});
    }

    return metricGroup;
}

GroupedMetricValues MonitoringPlan::fromBuffer(const std::shared_ptr<Schema>& schema, Runtime::TupleBuffer& buf) const {
    auto output = GroupedMetricValues();

    if (cpuMetrics) {
        auto metrics = std::make_unique<CpuMetrics>(CpuMetrics::fromBuffer(schema, buf, MonitoringPlan::CPU_METRICS_DESC));
        output.cpuMetrics.emplace(std::move(metrics));
    }
    if (diskMetrics) {
        auto metrics = std::make_unique<DiskMetrics>(DiskMetrics::fromBuffer(schema, buf, MonitoringPlan::DISK_METRICS_DESC));
        output.diskMetrics.emplace(std::move(metrics));
    }
    if (memoryMetrics) {
        auto metrics =
            std::make_unique<MemoryMetrics>(MemoryMetrics::fromBuffer(schema, buf, MonitoringPlan::MEMORY_METRICS_DESC));
        output.memoryMetrics.emplace(std::move(metrics));
    }
    if (networkMetrics) {
        auto metrics =
            std::make_unique<NetworkMetrics>(NetworkMetrics::fromBuffer(schema, buf, MonitoringPlan::NETWORK_METRICS_DESC));
        output.networkMetrics.emplace(std::move(metrics));
    }

    return output;
}

bool MonitoringPlan::hasMetric(MetricValueType metric) const {
    switch (metric) {
        case CpuMetric: {
            return cpuMetrics;
        };
        case DiskMetric: {
            return diskMetrics;
        };
        case MemoryMetric: {
            return memoryMetrics;
        };
        case NetworkMetric: {
            return networkMetrics;
        };
        default: {
            return false;
        }
    }
}

SchemaPtr MonitoringPlan::createSchema() const {
    MetricGroupPtr metricGroup = createMetricGroup(MetricCatalog::NesMetrics());
    return metricGroup->createSchema();
}

SerializableMonitoringPlan MonitoringPlan::serialize() const {
    SerializableMonitoringPlan serPlan;
    serPlan.set_cpumetrics(cpuMetrics);
    serPlan.set_diskmetrics(diskMetrics);
    serPlan.set_memorymetrics(memoryMetrics);
    serPlan.set_networkmetrics(networkMetrics);
    return serPlan;
}

std::string MonitoringPlan::toString() const {
    return "MonitoringPlan: CPU(" + std::to_string(cpuMetrics) + "), disk(" + std::to_string(diskMetrics) + "), " + "memory("
        + std::to_string(memoryMetrics) + "), network(" + std::to_string(networkMetrics) + ")";
}

std::ostream& operator<<(std::ostream& strm, const MonitoringPlan& plan) { return strm << plan.toString(); }

}// namespace NES
