#ifndef NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_

#include <cstdint>
#include <memory>
#include <vector>

namespace NES {
class Schema;
class BufferManager;
class TupleBuffer;

class CpuValues {
  public:
    CpuValues() = default;

    std::pair<std::shared_ptr<Schema>, TupleBuffer> serialize(std::shared_ptr<BufferManager> bm, const std::string& prefix="") const;

    void serialize(std::shared_ptr<Schema>, TupleBuffer, const std::string& prefix="") const;

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

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_
