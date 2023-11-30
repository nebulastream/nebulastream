/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_RUNTIME_INCLUDE_MONITORING_METRICS_GAUGE_RUNTIMEMETRICS_HPP_
#define NES_RUNTIME_INCLUDE_MONITORING_METRICS_GAUGE_RUNTIMEMETRICS_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES {

namespace Configurations {
class SchemaType;
using SchemaTypePtr = std::shared_ptr<SchemaType>;
}// namespace Configurations

namespace Monitoring {

/**
 * @brief Wrapper class to represent the metrics read from the OS about dynamically changing data represented in other metric classes.
 */
class RuntimeMetrics {
  public:
    RuntimeMetrics();

    /**
     * @brief Returns the schema of the class with a given prefix.
     * @param prefix
     * @return the schema
     */
    static Configurations::SchemaTypePtr getSchemaType(const std::string& prefix = "");

    /**
     * @brief Returns the schema of the class with a given prefix.
     * @param prefix
     * @return the schema
     */
    static SchemaPtr getSchema(const std::string& prefix);

    /**
     * @brief Writes a metrics objects to the given TupleBuffer and index.
     * @param buf the tuple buffer
     * @param tupleIndex the index indication its location in the buffer
    */
    void writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const;

    /**
     * @brief Parses a metrics objects from a TupleBuffer..
     * @param buf the tuple buffer
     * @param the tuple index indicating the location of the tuple
    */
    void readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex);

    /**
     * @brief Returns the metrics as json
     * @return Json containing the metrics
     */
    [[nodiscard]] nlohmann::json toJson() const;

    bool operator==(const RuntimeMetrics& rhs) const;
    bool operator!=(const RuntimeMetrics& rhs) const;

    uint64_t nodeId;
    uint64_t wallTimeNs;
    uint64_t memoryUsageInBytes;
    uint64_t cpuLoadInJiffies;//user+system
    uint64_t blkioBytesRead;
    uint64_t blkioBytesWritten;
    uint64_t batteryStatusInPercent;
    uint64_t latCoord;
    uint64_t longCoord;
} __attribute__((packed));

using RuntimeNesMetricsPtr = std::shared_ptr<RuntimeMetrics>;

/**
 * @brief Writes metrics objects to a given Schema and TupleBuffer.
 * @param the metrics
 * @param the TupleBuffer
 * @param the tuple index indicating the location of the tuple
*/
void writeToBuffer(const RuntimeMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses metrics objects from a given Schema and TupleBuffer.
 * @param the metrics
 * @param the TupleBuffer
 * @param the tuple index indicating the location of the tuple
*/
void readFromBuffer(RuntimeMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses the metric to JSON
 * @param metrics
 * @return the metrics as JSON
 */
nlohmann::json asJson(const RuntimeMetrics& metrics);

}// namespace Monitoring
}// namespace NES
#endif  // NES_RUNTIME_INCLUDE_MONITORING_METRICS_GAUGE_RUNTIMEMETRICS_HPP_
