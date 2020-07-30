#ifndef NES_INCLUDE_MONITORING_METRICS_COUNTER_HPP_
#define NES_INCLUDE_MONITORING_METRICS_COUNTER_HPP_

#include <Monitoring/Metrics/Metric.hpp>
#include <memory>

namespace NES {

/**
 * @brief A monitoring metric class that represents counters.
 */
class Counter : public Metric {
  public:
    Counter() : Metric(MetricType::CounterType){};

    /**
	 * Increment the current count by 1.
	 */
    virtual void inc() = 0;

    /**
     * Decrement the current count by 1.
     */
    virtual void dec() = 0;
};

typedef std::shared_ptr<Counter> CounterPtr;

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICS_COUNTER_HPP_
