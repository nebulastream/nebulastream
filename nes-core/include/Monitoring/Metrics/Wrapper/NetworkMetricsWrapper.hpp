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
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The object
    */
    void writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const;

    /**
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The object
    */
    void readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex);

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
 * @brief The serialize method to write CpuMetrics into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the CpuMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
*/
void writeToBuffer(const NetworkMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
 * @param schema
 * @param buf
 * @param prefix
 * @return The object
*/
void readFromBuffer(NetworkMetricsWrapper& wrapper, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

}// namespace NES

#endif//NES_NES_CORE_INCLUDE_MONITORING_METRICS_WRAPPER_NETWORKMETRICSWRAPPER_HPP
