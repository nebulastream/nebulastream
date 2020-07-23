#ifndef NES_INCLUDE_MONITORING_METRICVALUES_CPUSTATS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_CPUSTATS_HPP_

#include <cstdint>

namespace NES {

class CpuStats {
  public:
    CpuStats() = default;

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

}

#endif //NES_INCLUDE_MONITORING_METRICVALUES_CPUSTATS_HPP_
