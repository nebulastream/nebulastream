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

#ifndef ZMQSOURCE_HPP
#define ZMQSOURCE_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Sources/DataSource.hpp>

namespace NES {
class TupleBuffer;
/**
 * @brief this class provide a zmq as data source
 */
class ZmqSource : public DataSource {

  public:
    /**
     * @brief constructor for the zmq source
     * @param schema of the input buffer
     * @param host name of the source queue
     * @param port of the source queue
     */
    explicit ZmqSource(SchemaPtr schema, BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager, const std::string& host,
                       const uint16_t port, OperatorId operatorId);

    /**
     * @brief destructor of zmq sink that disconnects the queue before deconstruction
     * @note if queue cannot be disconnected, an assertion is raised
     */
    ~ZmqSource();

    /**
     * @brief blocking method to receive a buffer from the zmq source
     * @return TupleBufferPtr containing thre received buffer
     */
    std::optional<TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the zmq source
     * @return returns string describing the zmq source
     */
    const std::string toString() const override;

    /**
     * @brief The host address for the ZMQ
     * @return ZMQ host
     */
    const std::string& getHost() const;

    /**
     * @brief The port of the ZMQ
     * @return ZMQ port
     */
    uint16_t getPort() const;

    /**
     * @brief Get source type
     */
    SourceType getType() const override;

  private:
    /**
     * @brief default constructor required for boost serialization
     */
    ZmqSource();

    /**
     * @brief method to connect zmq using the host and port specified before
     * check if already connected, if not connect try to connect, if already connected return
     * @return bool indicating if connection could be established
     */
    bool connect();

    /**
     * @brief method to disconnect zmq
     * check if already disconnected, if not disconnected try to disconnect, if already disconnected return
     * @return bool indicating if connection could be established
     */
    bool disconnect();

    /**
     * @brief method for serialization, all listed variable below are added to the
     * serialization/deserialization process
     */
    friend class DataSource;
    std::string host;
    uint16_t port;
    bool connected;
    zmq::context_t context;
    zmq::socket_t socket;
};

typedef std::shared_ptr<ZmqSource> ZmqSourcePtr;
}// namespace NES
#endif// ZMQSOURCE_HPP
