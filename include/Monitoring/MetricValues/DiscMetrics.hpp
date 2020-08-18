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

    /**
     * @brief Returns the schema of the class with a given prefix.
     * @param prefix
     * @return the schema
     */
    static std::shared_ptr<Schema> getSchema(const std::string& prefix);

    uint64_t fBsize;
    uint64_t fFrsize;
    uint64_t fBlocks;
    uint64_t fBfree;
    uint64_t fBavail;
};

typedef std::shared_ptr<DiskMetrics> DiskMetricsPtr;

/**
 * @brief The serialize method to write DiskMetrics into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the DiskMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void serialize(DiskMetrics metrics, std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix);

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_DISCMETRICS_HPP_
