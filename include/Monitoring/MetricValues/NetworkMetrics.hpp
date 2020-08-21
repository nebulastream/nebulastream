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
class MetricDefinition;

class NetworkMetrics {

  public:
    NetworkMetrics() = default;

    // Overloading [] operator to access elements in array style
    NetworkValues& operator[](unsigned int interfaceNo);

    std::vector<std::string> getInterfaceNames();

    unsigned int getInterfaceNum() const;

    void addNetworkValues(NetworkValues&& nwValue);

    /**
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The object
     */
    static NetworkMetrics fromBuffer(std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix);

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
void serialize(NetworkMetrics metrics, std::shared_ptr<Schema> schema, TupleBuffer& buf, MetricDefinition& def,
               const std::string& prefix);

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_
