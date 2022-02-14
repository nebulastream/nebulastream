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

#ifndef NES_INCLUDE_MONITORING_METRICVALUES_STATICNESMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_STATICNESMETRICS_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

class SerializableRegistrationMetrics;
using SerializableRegistrationMetricsPtr = std::shared_ptr<SerializableRegistrationMetrics>;

namespace NES {

/**
 * Class representing the static metrics within NES.
 */
class RegistrationMetrics {
  public:
    RegistrationMetrics();
    RegistrationMetrics(bool isMoving, bool hasBattery);
    explicit RegistrationMetrics(SerializableRegistrationMetrics metrics);

    /**
     * @brief Returns the schema of the class with a given prefix.
     * @param prefix
     * @return the schema
     */
    static SchemaPtr getSchema(const std::string& prefix = "");

    /**
     * @brief Parses a CpuMetricsWrapper objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The object
     */
    static RegistrationMetrics fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& prefix);

    /**
     * @brief Returns the metrics as json
     * @return Json containing the metrics
     */
    [[nodiscard]] web::json::value toJson() const;

    /**
     * @brief Converts the object into a grpc protobuf object that can be serialized.
     * @return the serialized object as protobuf
     */
    [[nodiscard]] SerializableRegistrationMetricsPtr serialize() const;

    bool operator==(const RegistrationMetrics& rhs) const;
    bool operator!=(const RegistrationMetrics& rhs) const;

    uint64_t totalMemoryBytes;

    uint32_t cpuCoreNum;
    uint64_t totalCPUJiffies;//user+idle+system (Note: This value can change everytime it is read via AbstractSystemResourcesReader)

    // Using 1.5 CPUs is equivalent to --cpu-period="100000" and --cpu-quota="150000"
    int64_t cpuPeriodUS;//the CPU CFS scheduler period in microseconds
    int64_t cpuQuotaUS; // CPU CFS quota in microseconds

    bool isMoving;
    bool hasBattery;
} __attribute__((packed));

using RegistrationMetricsPtr = std::shared_ptr<RegistrationMetrics>;

/**
 * @brief The serialize method to write NodeRegistrationMetrics into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the CpuMetricsWrapper
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void writeToBuffer(const RegistrationMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset);

/**
 * @brief Class specific getSchema() method
 * @param metric
 * @param prefix
 * @return the SchemaPtr
 */
SchemaPtr getSchema(const RegistrationMetrics& metrics, const std::string& prefix);

}// namespace NES

#endif  // NES_INCLUDE_MONITORING_METRICVALUES_STATICNESMETRICS_HPP_
