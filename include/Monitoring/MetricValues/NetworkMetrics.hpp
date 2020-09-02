#ifndef NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_

#include <Monitoring/MetricValues/NetworkValues.hpp>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

namespace NES {
class Schema;
class TupleBuffer;
class MonitoringPlan;

typedef std::shared_ptr<Schema> SchemaPtr;

class NetworkMetrics {

  public:
    NetworkMetrics() = default;

    NetworkValues getNetworkValue(uint64_t interfaceNo) const;

    std::vector<std::string> getInterfaceNames();

    uint64_t getInterfaceNum() const;

    void addNetworkValues(NetworkValues&& nwValue);

    /**
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema The schema object, that will be extended with the new schema of the network metrics
     * @param buf The buffer where to data is written into
     * @param prefix A prefix that is appended to the schema fields
     * @return The NetworkMetrics object
     */
    static NetworkMetrics fromBuffer(SchemaPtr schema, TupleBuffer& buf, const std::string& prefix);

  private:
    std::vector<NetworkValues> networkValues;
};

/**
 * @brief The serialize method to write NetworkMetrics into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the NetworkMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void serialize(const NetworkMetrics& metrics, SchemaPtr schema, TupleBuffer& buf, const std::string& prefix);

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_
