#include <Monitoring/Metrics/Metric.hpp>
#include <Util/Logger.hpp>

namespace NES {

Metric::Metric(MetricType metricType) : metricType(metricType) {
}

MetricType Metric::getMetricType() const {
    return metricType;
}

}// namespace NES