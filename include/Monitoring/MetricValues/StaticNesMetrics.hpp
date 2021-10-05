/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_STATICNESMETRICS_HPP
#define NES_STATICNESMETRICS_HPP

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>

namespace NES {

class SerializableStaticNesMetrics;
using SerializableStaticNesMetricsPtr = std::shared_ptr<SerializableStaticNesMetrics>;

class StaticNesMetrics {
  public:
    StaticNesMetrics();

    /**
     * Ctor for the static NES metrics.
     * @param isMoving flag to indicate if the node is moving.
     * @param hasBattery flag to indicate if the node runs on battery.
     */
    StaticNesMetrics(bool isMoving, bool hasBattery);

    /**
     * Ctor for the static NES metrics.
     * @param isMoving flag to indicate if the node is moving.
     * @param hasBattery flag to indicate if the node runs on battery.
     */
    explicit StaticNesMetrics(SerializableStaticNesMetrics metrics);

    /**
     * @brief Returns the schema of the class with a given prefix.
     * @param prefix
     * @return the schema
     */
    static SchemaPtr getSchema(const std::string& prefix = "");

    /**
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The object
     */
    static StaticNesMetrics fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& prefix);


    /**
     * @brief Returns the metric as protobuf class
     * @return the SerializableStaticNesMetrics from Protobuf
     */
    SerializableStaticNesMetricsPtr toProtobufSerializable() const;

    /**
     * @brief Returns the metrics as json
     * @return Json containing the metrics
     */
    web::json::value toJson() const;

    bool operator==(const StaticNesMetrics& rhs) const;
    bool operator!=(const StaticNesMetrics& rhs) const;

    uint64_t totalMemoryBytes;

    uint32_t cpuCoreNum;
    uint64_t totalCPUJiffies;//user+idle+system (Note: This value can change everytime it is read via SystemResourcesReader)

    // Using 1.5 CPUs is equivalent to --cpu-period="100000" and --cpu-quota="150000"
    int64_t cpuPeriodUS;//the CPU CFS scheduler period in microseconds
    int64_t cpuQuotaUS; // CPU CFS quota in microseconds

    bool isMoving;
    bool hasBattery;
} __attribute__((packed));

using StaticNesMetricsPtr = std::shared_ptr<StaticNesMetrics>;

/**
 * @brief The serialize method to write StaticNesMetrics into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the CpuMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void writeToBuffer(const StaticNesMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset);

/**
 * @brief Class specific getSchema() method
 * @param metric
 * @param prefix
 * @return the SchemaPtr
 */
SchemaPtr getSchema(const StaticNesMetrics& metrics, const std::string& prefix);

}// namespace NES

#endif//NES_STATICNESMETRICS_HPP
