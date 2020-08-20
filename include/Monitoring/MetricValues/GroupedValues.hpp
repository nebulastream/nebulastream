#ifndef NES_INCLUDE_MONITORING_METRICVALUES_GROUPEDVALUES_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_GROUPEDVALUES_HPP_

#include <unordered_map>
#include <Monitoring/MetricValues/DiscMetrics.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>


namespace NES {
class MetricDefinition;

struct GroupedValues {
    void parseFromBuffer(std::shared_ptr<Schema> schema, TupleBuffer& buf, MetricDefinition def);

    std::unordered_map<std::string, DiskMetrics> diskMetrics;
    std::unordered_map<std::string, CpuMetrics> cpuMetrics;
    std::unordered_map<std::string, NetworkMetrics> networkMetrics;
    std::unordered_map<std::string, MemoryMetrics> memoryMetrics;
    std::unordered_map<std::string, Metric> simpleTypedMetrics;
};

}

#endif//NES_INCLUDE_MONITORING_METRICVALUES_GROUPEDVALUES_HPP_
