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

#ifndef NES_INCLUDE_MONITORING_METRICVALUES_RUNTIMENESMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_RUNTIMENESMETRICS_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES {

/**
 * @brief Wrapper class to represent the metrics read from the OS about cpu data.
 */
class RuntimeNesMetrics {
  public:
    RuntimeNesMetrics();

    /**
     * @brief Returns the schema of the class with a given prefix.
     * @param prefix
     * @return the schema
     */
    static SchemaPtr getSchema(const std::string& prefix = "");

    /**
     * @brief Parses a RuntimeNesMetrics objects based on a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The parsed RuntimeNesMetrics object
     */
    static RuntimeNesMetrics fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& prefix);

    /**
     * @brief Returns the metrics as json
     * @return Json containing the metrics
     */
    web::json::value toJson() const;

    bool operator==(const RuntimeNesMetrics& rhs) const;
    bool operator!=(const RuntimeNesMetrics& rhs) const;

    uint64_t wallTimeNs;
    uint64_t memoryUsageInBytes;
    uint64_t cpuLoadInJiffies;//user+system
    uint64_t blkioBytesRead;
    uint64_t blkioBytesWritten;
    uint64_t batteryStatusInPercent;
    uint64_t latCoord;
    uint64_t longCoord;
} __attribute__((packed));

using RuntimeNesMetricsPtr = std::shared_ptr<RuntimeNesMetrics>;

/**
 * @brief The serialize method to write runtime metrics of NES into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the runtime metrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void writeToBuffer(const RuntimeNesMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset);

/**
 * @brief Class specific getSchema() method for the runtime metrics
 * @param metric
 * @param prefix
 * @return the SchemaPtr
 */
SchemaPtr getSchema(const RuntimeNesMetrics& metrics, const std::string& prefix);

}// namespace NES

#endif  // NES_INCLUDE_MONITORING_METRICVALUES_RUNTIMENESMETRICS_HPP_
