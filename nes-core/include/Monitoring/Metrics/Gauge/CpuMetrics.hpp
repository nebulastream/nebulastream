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

#ifndef NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_CPUMETRICS_HPP_
#define NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_CPUMETRICS_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <string>
#include <vector>

namespace NES {

namespace Configurations {
class SchemaType;
using SchemaTypePtr = std::shared_ptr<SchemaType>;
}// namespace Configurations

namespace Monitoring {

/**
 * @brief This class represents the metrics read from /proc/stat:
    * %usr – % CPU usage at the user level
    * %nice – % CPU usage for user processes labeled “nice”
    * %sys – % CPU usage at the system (Linux kernel) level
    * %iowait – % CPU usage idling waiting on a disk read/write
    * %irq – % CPU usage handling hardware interrupts
    * %soft – % CPU usage handing software interrupts
    * %steal – % CPU usage being forced to wait for a hypervisor handling other virtual processors
    * %guest – % CPU usage spent running a virtual processor
    * %idle – % CPU usage on idle time (no processes, and not waiting on a disk read/write)
 */
class CpuMetrics {
  public:
    CpuMetrics();

    /**
     * @brief Returns the schema of the class with a given prefix.
     * @param prefix
     * @return the schema
     */
    static Configurations::SchemaTypePtr getSchemaType(const std::string& prefix);

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
     * @brief Source operator to convert the object to string
     * @param os
     * @param values
     * @return the source
     */
    friend std::ostream& operator<<(std::ostream& os, const CpuMetrics& values);

    /**
     * @brief Returns the metrics as json
     * @return Json containing the metrics
     */
    [[nodiscard]] nlohmann::json toJson() const;

    bool operator==(const CpuMetrics& rhs) const;
    bool operator!=(const CpuMetrics& rhs) const;

    uint64_t nodeId;
    uint64_t timestamp;
    uint64_t coreNum;
    uint64_t user;
    uint64_t nice;
    uint64_t system;
    uint64_t idle;
    uint64_t iowait;
    uint64_t irq;
    uint64_t softirq;
    uint64_t steal;
    uint64_t guest;
    uint64_t guestnice;
} __attribute__((packed));

/**
 * @brief The serialize method to write metrics into the given Schema and TupleBuffer.
 * @param the CpuMetrics
 * @param the TupleBuffer
 * @param the tuple index indicating the location of the tuple
 */
void writeToBuffer(const CpuMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses a metrics objects from a given Schema and TupleBuffer.
 * @param the CpuMetrics
 * @param the TupleBuffer
 * @param the tuple index indicating the location of the tuple
 * @return The object
*/
void readFromBuffer(CpuMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses the metric to JSON
 * @param metrics
 * @return the metrics as JSON
 */
nlohmann::json asJson(const CpuMetrics& metrics);

}// namespace Monitoring
}// namespace NES
#endif// NES_CORE_INCLUDE_MONITORING_METRICS_GAUGE_CPUMETRICS_HPP_
