#ifndef NES_INCLUDE_MONITORING_METRICVALUES_NETWORKVALUES_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_NETWORKVALUES_HPP_
#include <cstdint>
#include <memory>

namespace NES {
class Schema;
class TupleBuffer;
class MetricDefinition;

/**
 * @brief This class represents the metric values read from /proc/net/dev.
 */
class NetworkValues {
  public:
    NetworkValues() = default;

    /**
     * @brief Returns the schema of the class with a given prefix.
     * @param prefix
     * @return the schema
     */
    static std::shared_ptr<Schema> getSchema(const std::string& prefix);

    uint64_t rBytes;
    uint64_t rPackets;
    uint64_t rErrs;
    uint64_t rDrop;
    uint64_t rFifo;
    uint64_t rFrame;
    uint64_t rCompressed;
    uint64_t rMulticast;

    uint64_t tBytes;
    uint64_t tPackets;
    uint64_t tErrs;
    uint64_t tDrop;
    uint64_t tFifo;
    uint64_t tColls;
    uint64_t tCarrier;
    uint64_t tCompressed;
};

/**
 * @brief The serialize method to write NetworkValues into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the CpuMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void serialize(NetworkValues metrics, std::shared_ptr<Schema> schema, TupleBuffer& buf, MetricDefinition& def, const std::string& prefix);

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_NETWORKVALUES_HPP_
