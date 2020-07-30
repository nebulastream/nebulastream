#ifndef NES_INCLUDE_MONITORING_METRICVALUES_DISCMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_DISCMETRICS_HPP_

#include <cstdint>

namespace NES {

class DiskMetrics {
  public:
    DiskMetrics() = default;

    uint64_t F_BSIZE;
    uint64_t F_FRSIZE;
    uint64_t F_BLOCKS;
    uint64_t F_BFREE;
    uint64_t F_BAVAIL;
};

typedef std::shared_ptr<DiskMetrics> DiskMetricsPtr;

}


#endif //NES_INCLUDE_MONITORING_METRICVALUES_DISCMETRICS_HPP_
