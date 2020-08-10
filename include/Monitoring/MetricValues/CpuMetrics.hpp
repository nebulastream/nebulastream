#ifndef NES_INCLUDE_MONITORING_METRICVALUES_CPUMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_CPUMETRICS_HPP_

#include <Monitoring/MetricValues/CpuValues.hpp>
#include <memory>
#include <vector>

namespace NES {
class TupleBuffer;
class Schema;
class BufferManager;

/**
 * @brief Wrapper class to represent the metrics read from the OS about cpu data.
 */
class CpuMetrics {
  public:
    explicit CpuMetrics(CpuValues total, unsigned int size, std::unique_ptr<CpuValues[]> arr);

    /**
     * @brief Returns the cpu metrics for a given core
     * @param cpuCore core number
     * @return the cpu metrics
     */
    CpuValues getValues(unsigned int cpuCore) const;

    /**
     * @brief Returns the total cpu metrics summed up across all cores
     * @return The cpu values for all cores
     */
    CpuValues getTotal() const;

    /**
     * @brief The destructor to deallocate space.
     */
    ~CpuMetrics();

    /**
     * @brief Returns the number of cores of the node
     * @return core numbers
     */
    uint16_t getCpuNo() const;

    /**
     * @brief
     * @return
     */
    void serialize(std::shared_ptr<Schema> schema, TupleBuffer buf, const std::string& prefix="");

  private:
    // Overloading [] operator to access elements in array style
    CpuValues& operator[](unsigned int);

  private:
    CpuValues total;
    std::unique_ptr<CpuValues[]> ptr;
    const uint16_t cpuNo;
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_CPUMETRICS_HPP_
