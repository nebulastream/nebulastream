#include <Monitoring/Metrics/MetricGroup.hpp>

#include <Monitoring/Metrics/Counter.hpp>
#include <Monitoring/Metrics/Gauge.hpp>

namespace NES {

std::shared_ptr<MetricGroup> MetricGroup::create() {
    return std::shared_ptr<MetricGroup>();
}

bool MetricGroup::addMetric(const std::string& name, Metric* metric) {
    metricMap.insert(std::make_pair(name, metric));
    return true;
}

MetricPtr MetricGroup::getRegisteredMetric(const std::string& name) const {
    return this->metricMap.at(name);
}

}// namespace NES