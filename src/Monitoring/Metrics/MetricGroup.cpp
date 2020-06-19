#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Util/Logger.hpp>

namespace NES {

MetricGroup::MetricGroup() {
    NES_DEBUG("MetricGroup: Initialized");
}

MetricGroup::~MetricGroup() {
    NES_DEBUG("MetricGroup: Destroyed");
}

Counter& MetricGroup::counter(uint64_t id, Counter& counter) {
    metricMap[id] = static_cast<Metric&>(counter);
    return counter;
}

template<typename T>
Gauge<T>& MetricGroup::gauge(uint64_t id, Gauge<T>& gauge) {
    metricMap.insert(id, gauge);
    return gauge;
}

std::unordered_map<uint64_t, Metric&>& MetricGroup::getRegisteredMetrics() {
    return metricMap;
}

}