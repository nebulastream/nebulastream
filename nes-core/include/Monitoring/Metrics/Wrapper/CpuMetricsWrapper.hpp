#ifndef NES_NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_CPUMETRICSWRAPPER_HPP_
#define NES_NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_CPUMETRICSWRAPPER_HPP_

#include "Monitoring/MonitoringForwardRefs.hpp"
#include "Runtime/RuntimeForwardRefs.hpp"
#include "Monitoring/Metrics/Gauge/CpuMetrics.hpp"

namespace NES {

/**
 * @brief Wrapper class to represent the metrics read from the OS about cpu data.
 */
class CpuMetricsWrapper {
  public:
    CpuMetricsWrapper() = default;
    CpuMetricsWrapper(std::vector<CpuMetrics>&& arr);

    /**
     * @brief Returns the cpu metrics for a given core
     * @param cpuCore core number
     * @return the cpu metrics
    */
    [[nodiscard]] CpuMetrics getValue(const unsigned int cpuCore) const;

    /**
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The object
    */
    CpuMetricsWrapper fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& schemaPrefix);

    uint64_t size() const;

    CpuMetrics getTotal() const;

    /**
     * @brief Returns the metrics as json
     * @return Json containing the metrics
    */
    web::json::value toJson();

    bool operator==(const CpuMetricsWrapper& rhs) const;
    bool operator!=(const CpuMetricsWrapper& rhs) const;

  private:
    std::vector<CpuMetrics> cpuMetrics;
} __attribute__((packed));

/**
 * @brief The serialize method to write CpuMetrics into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the CpuMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
*/
void writeToBuffer(const CpuMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset);

/**
 * @brief Class specific getSchema() method
 * @param metric
 * @param prefix
 * @return the SchemaPtr
*/
SchemaPtr getSchema(const CpuMetricsWrapper& metrics, const std::string& prefix);

}// namespace NES

#endif//NES_NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_CPUMETRICSWRAPPER_HPP_
