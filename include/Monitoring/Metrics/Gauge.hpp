#ifndef NES_INCLUDE_MONITORING_METRICS_GAUGE_HPP_
#define NES_INCLUDE_MONITORING_METRICS_GAUGE_HPP_

#include <Monitoring/Metrics/Metric.hpp>
#include <Util/Logger.hpp>
#include <functional>

namespace NES {

template <typename T>
/**
 * A Gauge is a metric that calculates a specific value at a point in time.
 */
class Gauge: public Metric {
  public:
    explicit Gauge(std::function<T()>&& probingFunc): Metric(MetricType::GaugeType), probingFunc(probingFunc) {
        NES_DEBUG("Gauge: Initializing");
    }

    /**
     * @brief Calculates and returns the measured value.
     * @return the value
     */
    T getValue() {
        return probingFunc();
    }

  private:
    std::function<T()> probingFunc;
};

}

#endif //NES_INCLUDE_MONITORING_METRICS_GAUGE_HPP_
