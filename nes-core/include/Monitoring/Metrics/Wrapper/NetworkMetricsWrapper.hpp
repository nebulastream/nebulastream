#ifndef NES_NES_CORE_INCLUDE_MONITORING_METRICS_WRAPPER_NETWORKMETRICSWRAPPER_HPP
#define NES_NES_CORE_INCLUDE_MONITORING_METRICS_WRAPPER_NETWORKMETRICSWRAPPER_HPP

#include <Monitoring/Metrics/Gauge/NetworkMetrics.hpp>
#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES {

/**
 * @brief Wrapper class to represent the metrics read from the OS about Network data.
 */
class NetworkMetricsWrapper {
  public:
    NetworkMetricsWrapper() = default;

    /**
     * @brief Returns the Network metrics for a given core
     * @param NetworkCore core number
     * @return the Network metrics
    */
    [[nodiscard]] NetworkMetrics getNetworkValue(uint64_t interfaceNo) const;

    void addNetworkMetrics(NetworkMetrics&& nwValue);

    [[nodiscard]] uint64_t size() const;

    std::vector<std::string> getInterfaceNames();

    /**
     * @brief Parses a NetworkMetrics objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The object
    */
    NetworkMetricsWrapper fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& schemaPrefix);

    /**
     * @brief Returns the metrics as json
     * @return Json containing the metrics
    */
    web::json::value toJson();

    bool operator==(const NetworkMetricsWrapper& rhs) const;
    bool operator!=(const NetworkMetricsWrapper& rhs) const;

  private:
    std::vector<NetworkMetrics> networkMetrics;
} __attribute__((packed));

/**
 * @brief The serialize method to write NetworkMetrics into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the NetworkMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
*/
void writeToBuffer(const NetworkMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset);

/**
 * @brief Class specific getSchema() method
 * @param metric
 * @param prefix
 * @return the SchemaPtr
*/
SchemaPtr getSchema(const NetworkMetricsWrapper& metrics, const std::string& prefix);

}// namespace NES

#endif//NES_NES_CORE_INCLUDE_MONITORING_METRICS_WRAPPER_NETWORKMETRICSWRAPPER_HPP
