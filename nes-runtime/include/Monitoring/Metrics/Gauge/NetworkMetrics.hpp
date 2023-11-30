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

#ifndef NES_RUNTIME_INCLUDE_MONITORING_METRICS_GAUGE_NETWORKMETRICS_HPP_
#define NES_RUNTIME_INCLUDE_MONITORING_METRICS_GAUGE_NETWORKMETRICS_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES {

namespace Configurations {
class SchemaType;
using SchemaTypePtr = std::shared_ptr<SchemaType>;
}// namespace Configurations

namespace Monitoring {

/**
 * @brief This class represents the metric values read from /proc/net/dev:
 * bytes The total number of bytes of data transmitted or received by the interface.
 * packets The total number of packets of data transmitted or received by the interface.
 * errs The total number of transmit or receive errors detected by the device driver.
 * fifo The number of FIFO buffer errors.
 * frame The number of packet framing errors.
 * colls The number of collisions detected on the interface.
 * compressed The number of compressed packets transmitted or received by the device driver. (This appears to be unused in the 2.2.15 kernel.)
 * carrier The number of carrier losses detected by the device driver.
 * multicast The number of multicast frames transmitted or received by the device driver.
 */
class NetworkMetrics {
  public:
    NetworkMetrics();

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
     * @brief Returns the metrics as json
     * @return Json containing the metrics
     */
    [[nodiscard]] nlohmann::json toJson() const;

    bool operator==(const NetworkMetrics& rhs) const;
    bool operator!=(const NetworkMetrics& rhs) const;

    uint64_t nodeId;
    uint64_t timestamp;
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
 * @brief Writes metrics objects to a given Schema and TupleBuffer.
 * @param the metrics
 * @param the TupleBuffer
 * @param the tuple index indicating the location of the tuple
*/
void writeToBuffer(const NetworkMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses metrics objects from a given Schema and TupleBuffer.
 * @param the metrics
 * @param the TupleBuffer
 * @param the tuple index indicating the location of the tuple
*/
void readFromBuffer(NetworkMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief Parses the metric to JSON
 * @param metrics
 * @return the metrics as JSON
 */
nlohmann::json asJson(const NetworkMetrics& metrics);

}// namespace Monitoring
}// namespace NES
#endif// NES_RUNTIME_INCLUDE_MONITORING_METRICS_GAUGE_NETWORKMETRICS_HPP_
