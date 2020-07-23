#ifndef NES_INCLUDE_MONITORING_METRICVALUES_MEMORYMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_MEMORYMETRICS_HPP_

#include <cstdint>

namespace NES {

class MemoryMetrics {
  public:
    MemoryMetrics() = default;

    uint64_t TOTAL_RAM;
    uint64_t TOTAL_SWAP;
    uint64_t FREE_RAM;
    uint64_t SHARED_RAM;
    uint64_t BUFFER_RAM;
    uint64_t FREE_SWAP;
    uint64_t TOTAL_HIGH;
    uint64_t FREE_HIGH;
    uint64_t PROCS;
    uint64_t MEM_UNIT;
    uint64_t LOADS_1MIN;
    uint64_t LOADS_5MIN;
    uint64_t LOADS_15MIN;
};

}


#endif //NES_INCLUDE_MONITORING_METRICVALUES_MEMORYMETRICS_HPP_
