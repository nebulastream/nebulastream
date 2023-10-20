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

#ifndef NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_MEMORYMETRICS_HPP_
#define NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_MEMORYMETRICS_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES {

namespace Configurations {
class SchemaType;
using SchemaTypePtr = std::shared_ptr<SchemaType>;
}// namespace Configurations

namespace Monitoring {

/**
 * @brief MemoryMetrics class, that is responsible for collecting and managing memory metrics.
 */
class MemoryMetrics {
  public:
    MemoryMetrics();

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

    //equality operators
    bool operator==(const MemoryMetrics& rhs) const;
    bool operator!=(const MemoryMetrics& rhs) const;

    uint64_t nodeId;
    uint64_t timestamp;
    uint64_t TOTAL_RAM;
    uint64_t TOTAL_SWAP;
    uint64_t FREE_RAM;
    uint64_t SHARED_RAM;
    uint64_t BUFFER_RAM;
    uint64_t FREE_SWAP;
    uint64_t TOTAL_HIGH;
    uint64_t FREE_HIGH;
    uint64_t PROCS;
    uint64_t MEM_UNIT;
    uint64_t LOADS_1MIN;
    uint64_t LOADS_5MIN;
    uint64_t LOADS_15MIN;
} __attribute__((packed));
using MemoryMetricsPtr = std::shared_ptr<MemoryMetrics>;

/**
 * @brief Writes metrics objects to a given Schema and TupleBuffer.
 * @param the metrics
 * @param the TupleBuffer
 * @param the tuple index indicating the location of the tuple
*/
void writeToBuffer(const MemoryMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses metrics objects from a given Schema and TupleBuffer.
 * @param the metrics
 * @param the TupleBuffer
 * @param the tuple index indicating the location of the tuple
*/
void readFromBuffer(MemoryMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses the metric to JSON
 * @param metrics
 * @return the metrics as JSON
 */
nlohmann::json asJson(const MemoryMetrics& metrics);

}// namespace Monitoring
}// namespace NES
#endif// NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_MEMORYMETRICS_HPP_
