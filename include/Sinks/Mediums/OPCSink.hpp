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

#ifndef OPCSINK_HPP
#define OPCSINK_HPP
#ifdef ENABLE_OPC_BUILD

#include <Sinks/Mediums/SinkMedium.hpp>
#include <cstdint>
#include <memory>
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>
#include <string>

namespace NES {

/**
 * @brief class that allows writing a node to an OPC Server
 */
class OPCSink : public SinkMedium {

  public:
    /**
     * @brief constructor that saves data inside a node id on the server reached via provided url
     * @param format of the data
     * @param url is the server's address
     * @param nodeId gives the location for saving
     * @param user name to access the server
     * @param password to access the server
     */
    OPCSink(SinkFormatPtr format, std::string url, UA_NodeId nodeId, std::string user, std::string password);

    /**
     * @brief dtor
     */
    ~OPCSink();

    /**
     * @brief method to write a TupleBuffer
     * @param input_buffer a tuple buffer's pointer
     * @return bool indicating if the write was completed
     */
    bool writeData(TupleBuffer& input_buffer, WorkerContextRef) override;

    /**
     * @brief method to override virtual setup function
     */
    void setup() override;

    /**
     * @brief method to override virtual shutdown function
     */
    void shutdown() override;

    /**
     * @brief override the toSting method for the OPC sink
     * @return returns string describing the OPC sink
     */
    const std::string toString() const override;

    /**
     *
     * @brief get url
     */
    const std::string getUrl() const;

    /**
    * @brief get desired OPC node id
    * @return OPC node id
    */
    UA_NodeId getNodeId() const;

    /**
     *
     * @brief get OPC server user name
     */
    const std::string getUser() const;

    /**
     *
     * @brief get OPC server password
     */
    const std::string getPassword() const;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType();

    /**
     * @brief to string method
     * @return string of OPC Sink
     */
    std::string toString() override;
    /**
     * @brief saves the current status code
     * @return status code of OPCServer
     */
    UA_StatusCode getRetval() const;

  private:
    /**
     * @brief default constructor required for boost serialization
     */
    OPCSink();

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

    const std::string url;
    UA_NodeId nodeId;
    const std::string user;
    const std::string password;
    UA_StatusCode retval;
    UA_Client* client;
    bool connected;
};
typedef std::shared_ptr<OPCSink> OPCSinkPtr;
}// namespace NES

#endif
#endif// OPCSINK_HPP
