#ifndef NES_INCLUDE_MONITORING_METRICVALUES_CPUMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_CPUMETRICS_HPP_

#include <vector>
#include <Monitoring/MetricValues/CpuValues.hpp>

namespace NES {

class CpuMetrics {
  public:
    explicit CpuMetrics(const unsigned int size);

    // Overloading [] operator to access elements in array style
    CpuValues& operator[](unsigned int);

    unsigned int size() const;

  private:
    CpuValues* ptr;
    const unsigned int cpuNo;

  public:
    CpuValues TOTAL;
};

}

#endif //NES_INCLUDE_MONITORING_METRICVALUES_CPUMETRICS_HPP_
