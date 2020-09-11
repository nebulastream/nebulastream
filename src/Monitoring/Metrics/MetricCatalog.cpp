#include <Monitoring/Metrics/MetricCatalog.hpp>

#include <Monitoring/Metrics/Metric.hpp>
#include <utility>

namespace NES {

MetricCatalog::MetricCatalog(std::map<MetricValueType, Metric> metrics) : metricValueTypeToMetricMap(std::move(metrics)) {
}

std::shared_ptr<MetricCatalog> MetricCatalog::create(std::map<MetricValueType, Metric> metrics) {
    return std::make_shared<MetricCatalog>(MetricCatalog(std::move(metrics)));
}

std::shared_ptr<MetricCatalog> MetricCatalog::NesMetrics() {
    auto nesMetrics = std::map<MetricValueType, Metric>();
    return create(nesMetrics);
}

bool MetricCatalog::add(MetricValueType type, const Metric&& metric) {
    if (metricValueTypeToMetricMap.count(type) > 0) {
        return false;
    } else {
        metricValueTypeToMetricMap.insert(std::pair<MetricValueType, Metric>(type, metric));
        return false;
    }
}

}// namespace NES