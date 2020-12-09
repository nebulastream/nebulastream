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

#ifndef NES_INCLUDE_MONITORING_METRICVALUES_NETWORKVALUES_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_NETWORKVALUES_HPP_
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <cstdint>
#include <memory>
#include <string>

namespace NES {
class Schema;
class MonitoringPlan;
typedef std::shared_ptr<Schema> SchemaPtr;

/**
 * @brief This class represents the metric values read from /proc/net/dev.
 */
class NetworkValues {
  public:
    NetworkValues() = default;

    /**
     * @brief Returns the schema of the class with a given prefix.
     * @param prefix
     * @return the schema
     */
    static SchemaPtr getSchema(const std::string& prefix);

    /**
     * @brief Parses a CpuMetrics objects from a given Schema and TupleBuffer.
     * @param schema
     * @param buf
     * @param prefix
     * @return The object
     */
    static NetworkValues fromBuffer(SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix);

    std::string interfaceName;

    uint64_t rBytes;
    uint64_t rPackets;
    uint64_t rErrs;
    uint64_t rDrop;
    uint64_t rFifo;
    uint64_t rFrame;
    uint64_t rCompressed;
    uint64_t rMulticast;

    uint64_t tBytes;
    uint64_t tPackets;
    uint64_t tErrs;
    uint64_t tDrop;
    uint64_t tFifo;
    uint64_t tColls;
    uint64_t tCarrier;
    uint64_t tCompressed;
};

/**
 * @brief The serialize method to write NetworkValues into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the CpuMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void serialize(const NetworkValues& metrics, SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix);

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_NETWORKVALUES_HPP_
