#ifndef NES_INCLUDE_MONITORING_METRICVALUES_CPUMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_CPUMETRICS_HPP_

#include <Monitoring/MetricValues/CpuValues.hpp>
#include <memory>
#include <vector>

namespace NES {

class CpuMetrics {
  public:
    explicit CpuMetrics(CpuValues total, unsigned int size, std::unique_ptr<CpuValues[]> arr);

    CpuValues getValues(unsigned int cpuCore) const;

    CpuValues getTotal() const;

    /**
     * @brief The destructor to deallocate space.
     */
    ~CpuMetrics();

    /**
     * @brief
     * @return
     */
    unsigned int size() const;

  private:
    // Overloading [] operator to access elements in array style
    CpuValues& operator[](unsigned int);

  private:
    CpuValues total;
    std::unique_ptr<CpuValues[]> ptr;
    const unsigned int cpuNo;
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_CPUMETRICS_HPP_
