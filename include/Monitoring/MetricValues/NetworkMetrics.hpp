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

#ifndef NES_INCLUDE_MONITORING_METRIC_VALUES_NETWORK_METRICS_HPP_
#define NES_INCLUDE_MONITORING_METRIC_VALUES_NETWORK_METRICS_HPP_

#include <Monitoring/MetricValues/NetworkValues.hpp>
#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>

namespace NES {

/**
 * @brief NetworkMetrics class, that is responsible for collecting and managing network metrics.
 */
class NetworkMetrics {

  public:
    NetworkMetrics();

    [[nodiscard]] NetworkValues getNetworkValue(uint64_t interfaceNo) const;

    std::vector<std::string> getInterfaceNames();

    [[nodiscard]] uint64_t getInterfaceNum() const;

    void addNetworkValues(NetworkValues&& nwValue);

    /**
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema The schema object, that will be extended with the new schema of the network metrics
     * @param buf The buffer where to data is written into
     * @param prefix A prefix that is appended to the schema fields
     * @return The NetworkMetrics object
     */
    static NetworkMetrics fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& prefix);

    /**
     * @brief Returns the metrics as json
     * @return Json containing the metrics
     */
    web::json::value toJson();

  private:
    uint64_t interfaceNum{0};
    std::vector<NetworkValues> networkValues;

  public:
    bool operator==(const NetworkMetrics& rhs) const;
    bool operator!=(const NetworkMetrics& rhs) const;
};

/**
 * @brief The serialize method to write NetworkMetrics into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the NetworkMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void writeToBuffer(const NetworkMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset);

/**
 * @brief Class specific getSchema() method for NetworkMetrics
 * @param metric
 * @param prefix
 * @return the SchemaPtr
 */
SchemaPtr getSchema(const NetworkMetrics& metric, const std::string& prefix);

}// namespace NES

#endif// NES_INCLUDE_MONITORING_METRIC_VALUES_NETWORK_METRICS_HPP_
