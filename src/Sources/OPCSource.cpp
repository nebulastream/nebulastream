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

#ifdef ENABLE_OPC_BUILD

#include <Sources/OPCSource.hpp>
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <open62541/plugin/log_stdout.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Operators/OperatorId.hpp>
#include <Util/Logger.hpp>
#include <open62541/types.h>

namespace NES {

OPCSource::OPCSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, std::string url,
                     UA_NodeId nodeId, std::string password, std::string user, OperatorId operatorId)
    : DataSource(schema, bufferManager, queryManager, operatorId), url(url), nodeId(nodeId), retval(UA_STATUSCODE_GOOD),
      client(UA_Client_new()), connected(false), user(user), password(password) {

    NES_DEBUG("OPCSOURCE  " << this << ": Init OPC Source to " << url << " with user and password.");
}

OPCSource::~OPCSource() {
    NES_DEBUG("OPCSource::~OPCSource()");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("OPCSOURCE  " << this << ": Destroy OPC Source");
    } else {
        NES_ERROR("OPCSOURCE  " << this << ": Destroy OPC Source failed cause it could not be disconnected");
        assert(0);
    }
}

std::optional<TupleBuffer> OPCSource::receiveData() {

    NES_DEBUG("OPCSOURCE::receiveData()  " << this << ": receiveData() ");
    if (connect()) {

        UA_Variant* val = new UA_Variant;
        retval = UA_Client_readValueAttribute(client, nodeId, val);
        auto buffer = bufferManager->getBufferBlocking();
        buffer.setNumberOfTuples(1);
        NES_DEBUG("OPCSOURCE::receiveData()  " << this << ": got buffer ");

        if (retval == UA_STATUSCODE_GOOD && UA_Variant_isScalar(val)) {

            NES_DEBUG("OPCSOURCE::receiveData() Value datatype is: " << val->type->typeName);
            std::memcpy(buffer.getBuffer(), val->data, buffer.getBufferSize());
            UA_delete(val, val->type);
            return buffer;
        } else {

            NES_ERROR("OPCSOURCE::receiveData() error: Could not retrieve data. Further inspection needed.");
            return std::nullopt;
        }

    } else {

        NES_ERROR("OPCSOURCE::receiveData(): Not connected!");
        return std::nullopt;
    }
}

const std::string OPCSource::toString() const {

    char* ident = (char*) UA_malloc(sizeof(char) * nodeId.identifier.string.length + 1);
    memcpy(ident, nodeId.identifier.string.data, nodeId.identifier.string.length);
    ident[nodeId.identifier.string.length] = '\0';

    std::stringstream ss;
    ss << "OPC_SOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "URL= " << url << ", ";
    ss << "NODE_INDEX= " << nodeId.namespaceIndex << ", ";
    ss << "NODE_IDENTIFIER= " << ident << ". ";

    return ss.str();
}

bool OPCSource::connect() {

    UA_ClientConfig_setDefault(UA_Client_getConfig(client));

    if (!connected) {

        NES_DEBUG("OPCSOURCE::connect(): was !conncect now connect " << this << ": connected");
        retval = UA_Client_connect(client, url.c_str());
        NES_DEBUG("OPCSOURCE::connect(): connected without user or password");
        NES_DEBUG("OPCSOURCE::connect(): use address " << url);

        if (retval != UA_STATUSCODE_GOOD) {

            UA_Client_delete(client);
            connected = false;
            NES_ERROR("OPCSOURCE::connect(): ERROR with Status Code: " << retval << "OPCSOURCE " << this << ": set connected false");
        } else {

            connected = true;
        }
    }

    if (connected) {
        NES_DEBUG("OPCSOURCE::connect():  " << this << ": connected");
    } else {
        NES_DEBUG("Exception: OPCSOURCE::connect():  " << this << ": NOT connected");
    }
    return connected;
}

bool OPCSource::disconnect() {
    NES_DEBUG("OPCSource::disconnect() connected=" << connected);
    if (connected) {

        NES_DEBUG("OPCSOURCE::disconnect() disconnect client");
        UA_Client_disconnect(client);
        NES_DEBUG("OPCSOURCE::disconnect() delete client");
        UA_Client_delete(client);
        connected = false;
    }
    if (!connected) {
        NES_DEBUG("OPCSOURCE::disconnect()  " << this << ": disconnected");
    } else {
        NES_DEBUG("OPCSOURCE::disconnect()  " << this << ": NOT disconnected");
    }
    return !connected;
}

SourceType OPCSource::getType() const { return OPC_SOURCE; }

const std::string OPCSource::getUrl() const {
    return url;
}

UA_NodeId OPCSource::getNodeId() const {
    return nodeId;
}

const std::string OPCSource::getUser() const {
    return user;
}

const std::string OPCSource::getPassword() const {
    return password;
}

}// namespace NES
#endif