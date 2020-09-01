#ifndef NES_INCLUDE_MONITORING_METRICVALUES_GROUPEDVALUES_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_GROUPEDVALUES_HPP_

#include <Monitoring/MetricValues/DiscMetrics.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>


namespace NES {
class MonitoringPlan;

struct GroupedValues {
    std::optional<std::unique_ptr<DiskMetrics>> diskMetrics;
    std::optional<std::unique_ptr<CpuMetrics>> cpuMetrics;
    std::optional<std::unique_ptr<NetworkMetrics>> networkMetrics;
    std::optional<std::unique_ptr<MemoryMetrics>> memoryMetrics;
};

}

#endif//NES_INCLUDE_MONITORING_METRICVALUES_GROUPEDVALUES_HPP_
