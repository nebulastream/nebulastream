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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_ZMQSINKDESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_ZMQSINKDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Util/FaultToleranceType.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical zmq sink
 */
class ZmqSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief Creates the ZMQ sink description
     * @param host: address name for connecting to zmq
     * @param port: port number for connecting to zmq
     * @param internal: defines if the zmq should send the message schema as a first message
     * @param faultToleranceType: fault tolerance type of a query
     * @param numberOfSources: number of sources of a given query
     * @return descriptor for ZMQ sink
     */
    static SinkDescriptorPtr create(std::string host,
                                    uint16_t port,
                                    bool internal = false,
                                    FaultToleranceType faultToleranceType = FaultToleranceType::NONE,
                                    uint64_t numberOfSources = 0);

    /**
     * @brief Get the zmq address where the data is to be written
     */
    const std::string& getHost() const;

    /**
     * @brief Get the zmq port used for connecting to the server
     */
    uint16_t getPort() const;

    /**
     * Set the zmq port information
     * @param port : zmq port number
     */
    void setPort(uint16_t port);

    /**
     * @brief getter for fault-tolerance type
     * @return fault-tolerance type
     */
    FaultToleranceType getFaultToleranceType() const;

    bool isInternal() const;
    void setInternal(bool internal);

    [[nodiscard]] bool equal(SinkDescriptorPtr const& other) override;
    std::string toString() override;

    /**
     * @brief getter for number of sources
     * @return number of sources
     */
    uint64_t getNumberOfSources() const;

  private:
    explicit ZmqSinkDescriptor(std::string host,
                               uint16_t port,
                               bool internal,
                               FaultToleranceType faultToleranceType,
                               uint64_t numberOfSources);

    std::string host;
    uint16_t port;
    bool internal;
    FaultToleranceType faultToleranceType;
    uint64_t numberOfSources;
};

using ZmqSinkDescriptorPtr = std::shared_ptr<ZmqSinkDescriptor>;

}// namespace NES

#endif// NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_ZMQSINKDESCRIPTOR_HPP_
