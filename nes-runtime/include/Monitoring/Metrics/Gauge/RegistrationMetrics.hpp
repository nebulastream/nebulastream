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

#ifndef NES_RUNTIME_INCLUDE_MONITORING_METRICS_GAUGE_REGISTRATIONMETRICS_HPP_
#define NES_RUNTIME_INCLUDE_MONITORING_METRICS_GAUGE_REGISTRATIONMETRICS_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

class SerializableRegistrationMetrics;
using SerializableRegistrationMetricsPtr = std::shared_ptr<SerializableRegistrationMetrics>;

namespace NES {

namespace Configurations {
class SchemaType;
using SchemaTypePtr = std::shared_ptr<SchemaType>;
}// namespace Configurations

namespace Monitoring {

/**
 * Class representing the static metrics that are transmitted during node registration in NES.
 * totalMemoryBytes; the total available memory on the node in bytes
 * cpuCoreNum; number of cores
 * totalCPUJiffies; user+idle+system (Note: This value can change everytime it is read via AbstractSystemResourcesReader) Using 1.5 CPUs is equivalent to --cpu-period="100000" and --cpu-quota="150000"
 * cpuPeriodUS; the CPU CFS scheduler period in microseconds
 * cpuQuotaUS; CPU CFS quota in microseconds
 * isMoving; flag to indicate if the node is changing geo-location
 * hasBattery; flag to indicate if the node is running on a battery
 */
class RegistrationMetrics {
  public:
    RegistrationMetrics();
    RegistrationMetrics(bool isMoving, bool hasBattery);
    /**
     * Ctor to create a RegistrationMetrics object out of a protobuf message.
     * @param metrics in protobuf
     */
    explicit RegistrationMetrics(const SerializableRegistrationMetrics& metrics);

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

    /**
     * @brief Converts the object into a grpc protobuf object that can be serialized.
     * @return the serialized object as protobuf
     */
    [[nodiscard]] SerializableRegistrationMetricsPtr serialize() const;

    bool operator==(const RegistrationMetrics& rhs) const;
    bool operator!=(const RegistrationMetrics& rhs) const;

    uint64_t nodeId;
    uint64_t totalMemoryBytes;
    uint64_t cpuCoreNum;
    uint64_t totalCPUJiffies;//user+idle+system (This value can change everytime it is read via AbstractSystemResourcesReader)
    // Using 1.5 CPUs is equivalent to --cpu-period="100000" and --cpu-quota="150000"
    int64_t cpuPeriodUS;//the CPU CFS scheduler period in microseconds
    int64_t cpuQuotaUS; // CPU CFS quota in microseconds

    bool isMoving;
    bool hasBattery;
} __attribute__((packed));

using RegistrationMetricsPtr = std::shared_ptr<RegistrationMetrics>;

/**
 * @brief The serialize method to write metrics into the given Schema and TupleBuffer.
 * @param the CpuMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void writeToBuffer(const RegistrationMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses a metrics objects from a given Schema and TupleBuffer.
 * @param schema
 * @param buf
 * @param prefix
 * @return The object
*/
void readFromBuffer(RegistrationMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses the metric to JSON
 * @param metrics
 * @return the metrics as JSON
 */
nlohmann::json asJson(const RegistrationMetrics& metrics);

}// namespace Monitoring
}// namespace NES
#endif // NES_RUNTIME_INCLUDE_MONITORING_METRICS_GAUGE_REGISTRATIONMETRICS_HPP_
