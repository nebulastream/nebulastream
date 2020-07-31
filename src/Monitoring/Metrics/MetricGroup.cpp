#include <Monitoring/Metrics/MetricGroup.hpp>

#include <Monitoring/Metrics/Counter.hpp>
#include <Monitoring/Metrics/Gauge.hpp>

namespace NES {

std::shared_ptr<MetricGroup> MetricGroup::create() {
    return std::shared_ptr<MetricGroup>();
}

bool MetricGroup::addMetric(std::shared_ptr<Metric> metric) {
    //metricMap.emplace_back(std::move(metric));
    return true;
}

Metric MetricGroup::getRegisteredMetric(const std::string& name) const {
    //return this->metricMap.at(name);
}

}// namespace NES