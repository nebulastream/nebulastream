#ifndef NES_INCLUDE_MONITORING_METRICS_GAUGE_HPP_
#define NES_INCLUDE_MONITORING_METRICS_GAUGE_HPP_

#include <Monitoring/Metrics/Metric.hpp>

namespace NES {

template <typename T>
class Gauge: public Metric {
  public:
    explicit Gauge(T value);

    /**
     * @brief getter for the value
     * @return the value
     */
    T getValue();

  private:
    T value;
};

}

#endif //NES_INCLUDE_MONITORING_METRICS_GAUGE_HPP_
