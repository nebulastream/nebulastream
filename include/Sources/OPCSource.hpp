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

#ifndef OPCSOURCE_HPP
#define OPCSOURCE_HPP
#ifdef ENABLE_OPC_BUILD

#include <Sources/DataSource.hpp>
#include <cstdint>
#include <memory>
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>
#include <string>

namespace NES {
class TupleBuffer;

/**
 * @brief this class provide a zmq as data source
 */
class OPCSource : public DataSource {

  public:
    /**
     * @brief constructor for the opc source
     * @param schema schema of the elements
     * @param url the url of the OPC server
     * @param nodeId the node id of the desired node
     * @param user name if connecting with a server with authentication
     * @param password for authentication if needed
     */
    OPCSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
              std::string url, UA_NodeId nodeId, std::string password, std::string user, OperatorId operatorId);

    /**
     * @brief destructor of OPC source that disconnects the queue before deconstruction
     * @note if queue cannot be disconnected, an assertion is raised
     */
    ~OPCSource();

    /**
     * @brief blocking method to receive a buffer from the OPC source
     * @return TupleBufferPtr containing the received buffer
     */
    std::optional<NodeEngine::TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the opc source
     * @return returns string describing the opc source
     */
    const std::string toString() const override;

    /**
     * @brief The url of the OPC server
     * @return OPC url
     */
    const std::string getUrl() const;

    /**
     * @brief get desired OPC node id
     * @return OPC node id
     */
    UA_NodeId getNodeId() const;

    /**
     * @brief get user name for OPC server
     * @return opc server user name
     */
    const std::string getUser() const;

    /**
     * @brief get password for OPC server
     * @return opc server password
     */
    const std::string getPassword() const;

    /**
     * @brief Get source type
     */
    SourceType getType() const override;

  private:
    /**
     * @brief default constructor required for boost serialization
     */
    OPCSource() = default;

    /**
     * @brief method to connect opc using the url specified before
     * check if already connected, if not connect try to connect, if already connected return
     * @return bool indicating if connection could be established
     */
    bool connect();

    /**
     * @brief method to disconnect opc using the url specified before
     * check if connected, if connected try to disconnect, if not connected return
     * @return bool indicating if connection could be disconnected
     */
    bool disconnect();

    /**
     * @brief method for serialization, all listed variable below are added to the
     * serialization/deserialization process
     */
    friend class DataSource;

    const std::string url;
    UA_NodeId nodeId;
    const std::string user;
    const std::string password;
    UA_StatusCode retval;
    UA_Client* client;
    bool connected;
};

typedef std::shared_ptr<OPCSource> OPCSourcePtr;
}// namespace NES

#endif
#endif// OPCSOURCE_HPP
