#ifndef NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_

#include <memory>

namespace NES {
class Schema;
class TupleBuffer;

class CpuValues {
  public:
    CpuValues() = default;

    static std::shared_ptr<Schema> getSchema(const std::string& prefix);

    uint64_t USER;
    uint64_t NICE;
    uint64_t SYSTEM;
    uint64_t IDLE;
    uint64_t IOWAIT;
    uint64_t IRQ;
    uint64_t SOFTIRQ;
    uint64_t STEAL;
    uint64_t GUEST;
    uint64_t GUESTNICE;
};

void serialize(CpuValues, std::shared_ptr<Schema>, TupleBuffer&, const std::string& prefix="");

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_
