#ifndef NES_INCLUDE_MONITORING_METRICVALUES_DISCMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_DISCMETRICS_HPP_

#include <cstdint>
#include <memory>

namespace NES {
class Schema;
class TupleBuffer;

class DiskMetrics {
  public:
    DiskMetrics() = default;

    static std::shared_ptr<Schema> getSchema(const std::string& prefix);

    uint64_t F_BSIZE;
    uint64_t F_FRSIZE;
    uint64_t F_BLOCKS;
    uint64_t F_BFREE;
    uint64_t F_BAVAIL;
};

typedef std::shared_ptr<DiskMetrics> DiskMetricsPtr;

void serialize(DiskMetrics, std::shared_ptr<Schema>, TupleBuffer&, const std::string& prefix="");

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_DISCMETRICS_HPP_
