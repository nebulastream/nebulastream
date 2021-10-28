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

#ifndef NES_INCLUDE_MONITORING_METRIC_VALUES_NETWORK_VALUES_HPP_
#define NES_INCLUDE_MONITORING_METRIC_VALUES_NETWORK_VALUES_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>

namespace NES {

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
    static NetworkValues fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& prefix);

    /**
     * @brief Returns the metrics as json
     * @return Json containing the metrics
     */
    web::json::value toJson() const;

    bool operator==(const NetworkValues& rhs) const;
    bool operator!=(const NetworkValues& rhs) const;

    uint64_t interfaceName;

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
} __attribute__((packed));

/**
 * @brief The serialize method to write NetworkValues into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the CpuMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void writeToBuffer(const NetworkValues& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset);

}// namespace NES

#endif// NES_INCLUDE_MONITORING_METRIC_VALUES_NETWORK_VALUES_HPP_
