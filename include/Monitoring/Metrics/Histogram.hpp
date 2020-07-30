#ifndef NES_INCLUDE_MONITORING_METRICS_HISTOGRAM_HPP_
#define NES_INCLUDE_MONITORING_METRICS_HISTOGRAM_HPP_

#include <Monitoring/Metrics/Metric.hpp>
#include <Util/Logger.hpp>

namespace NES {

/**
 * @brief TODO: WIP
 */
class Histogram : public Metric {
  public:
    Histogram() : Metric(MetricType::HistogramType) {
        NES_NOT_IMPLEMENTED();
    }
};
}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICS_HISTOGRAM_HPP_
