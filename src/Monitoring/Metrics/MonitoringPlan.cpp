#include <Monitoring/Metrics/MonitoringPlan.hpp>

#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/DiscMetrics.hpp>
#include <Monitoring/MetricValues/GroupedValues.hpp>
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

MonitoringPlan::MonitoringPlan(const SerializableMonitoringPlan plan): cpuMetrics(plan.cpumetrics()), networkMetrics(plan.networkmetrics()),
                                                                        memoryMetrics(plan.memorymetrics()), diskMetrics(plan.diskmetrics()) {
    NES_DEBUG("MonitoringPlan: Initializing from shippable protobuf object.");
}

void MonitoringPlan::addMetric(MetricValueType metric) {
    switch (metric) {
        case CpuMetric: {
            cpuMetrics = true;
            break;
        };
        case DiskMetric: {
            diskMetrics = true;
            break;
        };
        case MemoryMetric: {
            memoryMetrics = true;
            break;
        };
        case NetworkMetric: {
            networkMetrics = true;
            break;
        };
    }
}

void MonitoringPlan::addMetrics(const std::vector<MetricValueType>& metrics) {
    for (auto& m : metrics) {
        addMetric(m);
    }
}

std::shared_ptr<MetricGroup> MonitoringPlan::createMetricGroup(MetricCatalogPtr) const {
    MetricGroupPtr metricGroup = MetricGroup::create();

    if (cpuMetrics) {
        NES_DEBUG("MonitoringPlan: Adding " + MonitoringPlan::CPU_METRICS_DESC + " to monitoring plan.");
        Gauge<CpuMetrics> cpuStats = MetricUtils::CPUStats();
        metricGroup->add(MonitoringPlan::CPU_METRICS_DESC, cpuStats);
    }
    if (diskMetrics) {
        NES_DEBUG("MonitoringPlan: Adding " + MonitoringPlan::DISK_METRICS_DESC + " to monitoring plan.");
        Gauge<DiskMetrics> diskStats = MetricUtils::DiskStats();
        metricGroup->add(MonitoringPlan::DISK_METRICS_DESC, diskStats);
    }
    if (memoryMetrics) {
        NES_DEBUG("MonitoringPlan: Adding " + MonitoringPlan::MEMORY_METRICS_DESC + " to monitoring plan.");
        Gauge<MemoryMetrics> memStats = MetricUtils::MemoryStats();
        metricGroup->add(MonitoringPlan::MEMORY_METRICS_DESC, memStats);
    }
    if (networkMetrics) {
        NES_DEBUG("MonitoringPlan: Adding " + MonitoringPlan::NETWORK_METRICS_DESC + " to monitoring plan.");
        Gauge<NetworkMetrics> networkStats = MetricUtils::NetworkStats();
        metricGroup->add(MonitoringPlan::NETWORK_METRICS_DESC, networkStats);
    }

    return metricGroup;
}

GroupedValues MonitoringPlan::fromBuffer(std::shared_ptr<Schema> schema, TupleBuffer& buf) {
    auto output = GroupedValues();

    if (cpuMetrics) {
        auto metrics = std::make_unique<CpuMetrics>(CpuMetrics::fromBuffer(schema, buf, MonitoringPlan::CPU_METRICS_DESC));
        output.cpuMetrics.emplace(std::move(metrics));
    }
    if (diskMetrics) {
        auto metrics = std::make_unique<DiskMetrics>(DiskMetrics::fromBuffer(schema, buf, MonitoringPlan::DISK_METRICS_DESC));
        output.diskMetrics.emplace(std::move(metrics));
    }
    if (memoryMetrics) {
        auto metrics = std::make_unique<MemoryMetrics>(MemoryMetrics::fromBuffer(schema, buf, MonitoringPlan::MEMORY_METRICS_DESC));
        output.memoryMetrics.emplace(std::move(metrics));
    }
    if (networkMetrics) {
        auto metrics = std::make_unique<NetworkMetrics>(NetworkMetrics::fromBuffer(schema, buf, MonitoringPlan::NETWORK_METRICS_DESC));
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

SerializableMonitoringPlan MonitoringPlan::serialize() const {
    SerializableMonitoringPlan splan;
    splan.set_cpumetrics(cpuMetrics);
    splan.set_diskmetrics(diskMetrics);
    splan.set_memorymetrics(memoryMetrics);
    splan.set_networkmetrics(networkMetrics);
    return splan;
}

std::ostream& operator<<(std::ostream &strm, const MonitoringPlan &plan) {
    return strm << "MonitoringPlan: CPU(" << std::to_string(plan.cpuMetrics) << "), disk(" << std::to_string(plan.diskMetrics) + "), " <<
        "memory(" <<  std::to_string(plan.memoryMetrics) << "), network(" << std::to_string(plan.networkMetrics) << ")";
}

}// namespace NES
