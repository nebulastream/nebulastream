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

#ifndef NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SINKS_ZMQ_SINK_DESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SINKS_ZMQ_SINK_DESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

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
     * @return descriptor for ZMQ sink
     */
    static SinkDescriptorPtr create(std::string host, uint16_t port, bool internal = false);

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

    bool isInternal() const;
    void setInternal(bool internal);

    [[nodiscard]] bool equal(SinkDescriptorPtr const& other) override;
    std::string toString() override;

  private:
    explicit ZmqSinkDescriptor(std::string host, uint16_t port, bool internal);

    std::string host;
    uint16_t port;
    bool internal;
};

using ZmqSinkDescriptorPtr = std::shared_ptr<ZmqSinkDescriptor>;

}// namespace NES

#endif// NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SINKS_ZMQ_SINK_DESCRIPTOR_HPP_
