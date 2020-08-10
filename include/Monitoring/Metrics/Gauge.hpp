#ifndef NES_INCLUDE_MONITORING_METRICS_GAUGE_HPP_
#define NES_INCLUDE_MONITORING_METRICS_GAUGE_HPP_

#include <Monitoring/Metrics/Gauge.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Util/Logger.hpp>
#include <functional>
#include <memory>

namespace NES {

template<typename T>
/**
 * A Gauge is a metric that calculates a specific value at a point in time.
 */
class Gauge {
  public:
    explicit Gauge(std::function<T()>&& probingFunc) : probingFunc(probingFunc) {
        NES_DEBUG("Gauge: Initializing");
    }

    /**
     * @brief Calculates and returns the measured value.
     * @return the value
     */
    T measure() {
        return probingFunc();
    }

  private:
    std::function<T()> probingFunc;
};

template<typename T>
MetricType getMetricType(const Gauge<T>&) {
    return MetricType::GaugeType;
}

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICS_GAUGE_HPP_
