#include <Monitoring/Metrics/MetricGroup.hpp>

#include <Monitoring/Metrics/Counter.hpp>
#include <Monitoring/Metrics/Gauge.hpp>

namespace NES {

bool MetricGroup::add(const std::string& name, Metric* metric) {
    metricMap[name] = metric;
}

std::unordered_map<std::string, Metric*> MetricGroup::getRegisteredMetrics() const {
    return this->metricMap;
}

}