#ifndef NES_INCLUDE_MONITORING_METRICS_INTCOUNTER_HPP_
#define NES_INCLUDE_MONITORING_METRICS_INTCOUNTER_HPP_

#include <Monitoring/Metrics/Counter.hpp>
#include <cstdint>

namespace NES {

/**
 * @brief A monitoring metric class that represents counters.
 */
class IntCounter : public Counter {
  public:
    explicit IntCounter(uint64_t initCount = 0);

    /**
	 * Increment the current count by 1.
	 */
    void inc() override;

    /**
     * Increment the current count by the given value.
     *
     * @param n value to increment the current count by
     * @return the new value
     */
    uint64_t inc(uint64_t val);

    /**
     * Decrement the current count by 1.
     */
    void dec() override;

    /**
     * Decrement the current count by the given value.
     *
     * @param n value to decrement the current count by
     */
    uint64_t dec(uint64_t val);

    /**
     * Returns the current count.
     *
     * @return current count
     */
    uint64_t getCount() const;

  private:
    uint64_t count;
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICS_INTCOUNTER_HPP_
