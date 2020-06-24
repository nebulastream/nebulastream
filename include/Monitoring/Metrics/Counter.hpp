#ifndef NES_INCLUDE_MONITORING_METRICS_COUNTER_HPP_
#define NES_INCLUDE_MONITORING_METRICS_COUNTER_HPP_

#include <Monitoring/Metrics/Metric.hpp>
#include <cstdint>

namespace NES {

/**
 * @brief A monitoring metric class that represents counters.
 */
class Counter: public Metric {
  public:
    explicit Counter(int64_t initCount=0);

    /**
	 * Increment the current count by 1.
	 */
    void inc();

    /**
     * Increment the current count by the given value.
     *
     * @param n value to increment the current count by
     */
    void inc(int64_t n);

    /**
     * Decrement the current count by 1.
     */
    void dec();

    /**
     * Decrement the current count by the given value.
     *
     * @param n value to decrement the current count by
     */
    void dec(int64_t n);

    /**
     * Returns the current count.
     *
     * @return current count
     */
    int64_t getCount() const;

  private:
    int64_t count;

};

}

#endif //NES_INCLUDE_MONITORING_METRICS_COUNTER_HPP_
