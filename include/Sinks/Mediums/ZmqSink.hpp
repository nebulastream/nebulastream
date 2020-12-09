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

#ifndef ZMQSINK_HPP
#define ZMQSINK_HPP

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
    ~ZmqSink();

    bool writeData(TupleBuffer& input_buffer, NodeEngine::WorkerContextRef) override;
    void setup() override { connect(); };
    void shutdown() override{};
    const std::string toString() const override;

    /**
     * @brief Get zmq sink port
     */
    int getPort();

    /**
     * @brief Get Zmq host name
     */
    const std::string getHost() const;

    /**
     * @brief Get Sink type
     */
    std::string toString() override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType();

  private:
    ZmqSink();

    std::string host;
    uint16_t port;

    bool connected;
    zmq::context_t context;
    zmq::socket_t socket;
    bool internal;

    bool connect();
    bool disconnect();
};
typedef std::shared_ptr<ZmqSink> ZmqSinkPtr;
}// namespace NES

#endif// ZMQSINK_HPP
