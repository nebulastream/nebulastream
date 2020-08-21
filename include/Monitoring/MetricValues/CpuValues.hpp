#ifndef NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_

#include <memory>

namespace NES {
class Schema;
class TupleBuffer;
class MetricDefinition;

/**
 * @brief This class represents the metrics read from /proc/stat.
 */
class CpuValues {
  public:
    CpuValues() = default;

    /**
     * @brief Returns the schema of the class with a given prefix.
     * @param prefix
     * @return the schema
     */
    static std::shared_ptr<Schema> getSchema(const std::string& prefix);

    /**
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The object
     */
    static CpuValues fromBuffer(std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix);

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

/**
 * @brief The serialize method to write CpuValues into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the CpuValues
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void serialize(CpuValues metrics, std::shared_ptr<Schema> schema, TupleBuffer& buf, MetricDefinition& def, const std::string& prefix);

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_CPUVALUES_HPP_
