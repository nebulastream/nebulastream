#include <Monitoring/Metrics/MetricGroup.hpp>

#include <Monitoring/Metrics/Counter.hpp>
#include <Monitoring/Metrics/Gauge.hpp>

namespace NES {

std::shared_ptr<MetricGroup> MetricGroup::create() {
    return std::make_shared<MetricGroup>(MetricGroup());
}

bool MetricGroup::add(const std::string& desc, const Metric& metric) {
    return metricMap.insert(std::make_pair(desc, metric)).second;
}

bool MetricGroup::remove(const std::string& name) {
    return metricMap.erase(name);
}

}// namespace NES