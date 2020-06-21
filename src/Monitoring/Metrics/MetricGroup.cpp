#include <Monitoring/Metrics/MetricGroup.hpp>

#include <Monitoring/Metrics/Counter.hpp>
#include <Monitoring/Metrics/Gauge.hpp>

namespace NES {

bool MetricGroup::add(const std::string& id, Metric* metric) {
    metricMap[id] = metric;
}

std::unordered_map<std::string, Metric*> MetricGroup::getRegisteredMetrics() const {
    return this->metricMap;
}

}