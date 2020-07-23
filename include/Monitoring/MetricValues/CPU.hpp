#ifndef NES_INCLUDE_MONITORING_METRICVALUES_CPU_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_CPU_HPP_

#include <vector>
#include <Monitoring/MetricValues/CpuStats.hpp>

namespace NES {

class CPU {
  public:
    explicit CPU(const unsigned int size);

    // Overloading [] operator to access elements in array style
    CpuStats& operator[](unsigned int);

    unsigned int size() const;

  private:
    CpuStats* ptr;
    const unsigned int cpuNo;

  public:
    CpuStats TOTAL;
};

}

#endif //NES_INCLUDE_MONITORING_METRICVALUES_CPU_HPP_
