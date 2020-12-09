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

#ifndef NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_

#include <Monitoring/MetricValues/NetworkValues.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

namespace NES {
class Schema;
class MonitoringPlan;

typedef std::shared_ptr<Schema> SchemaPtr;

class NetworkMetrics {

  public:
    NetworkMetrics() = default;

    NetworkValues getNetworkValue(uint64_t interfaceNo) const;

    std::vector<std::string> getInterfaceNames();

    uint64_t getInterfaceNum() const;

    void addNetworkValues(NetworkValues&& nwValue);

    /**
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema The schema object, that will be extended with the new schema of the network metrics
     * @param buf The buffer where to data is written into
     * @param prefix A prefix that is appended to the schema fields
     * @return The NetworkMetrics object
     */
    static NetworkMetrics fromBuffer(SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix);

  private:
    std::vector<NetworkValues> networkValues;
};

/**
 * @brief The serialize method to write NetworkMetrics into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the NetworkMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void serialize(const NetworkMetrics& metrics, SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix);

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_
