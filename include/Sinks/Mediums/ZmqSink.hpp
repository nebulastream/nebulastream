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

#ifndef NES_INCLUDE_SINKS_MEDIUMS_ZMQ_SINK_HPP_
#define NES_INCLUDE_SINKS_MEDIUMS_ZMQ_SINK_HPP_

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Sinks/Mediums/SinkMedium.hpp>

namespace NES {

class ZmqSink : public SinkMedium {

  public:
    //TODO: remove internal flag once the new network stack is in place
    ZmqSink(SinkFormatPtr format, const std::string& host, uint16_t port, bool internal, QuerySubPlanId parentPlanId);
    ~ZmqSink() override;

    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) override;
    void setup() override { connect(); };
    void shutdown() override{};
    std::string toString() const override;

    /**
     * @brief Get zmq sink port
     */
    int getPort() const;

    /**
     * @brief Get Zmq address name
     */
    std::string getHost() const;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType() override;

  private:
    std::string host;
    uint16_t port;

    bool connected{false};
    bool internal;
    zmq::context_t context{zmq::context_t(1)};
    zmq::socket_t socket{zmq::socket_t(context, ZMQ_PUSH)};

    bool connect();
    bool disconnect();
};
using ZmqSinkPtr = std::shared_ptr<ZmqSink>;
}// namespace NES

#endif// NES_INCLUDE_SINKS_MEDIUMS_ZMQ_SINK_HPP_
