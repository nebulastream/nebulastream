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

#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <Util/Logger.hpp>
#include <open62541/types.h>

namespace NES {

OPCSource::OPCSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string &url,
                     UA_NodeId* nodeId, const std::string &password, const std::string &user)
    : DataSource(schema, bufferManager, queryManager), 
    url(url),
    nodeId(nodeId),
    retval(UA_STATUSCODE_GOOD),
    client(UA_Client_new()),
    connected(false),
    user(user),
    password(password)
    {

    NES_DEBUG(
        "OPCSOURCE  " << this << ": Init OPC Source to " << url << " with user and password.");
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
    NES_DEBUG("OPCSource  " << this << ": receiveData ");
    if (connect()) {
        UA_Int32 value = 0;
        UA_Variant *val = UA_Variant_new();

        NES_DEBUG("Init var");

        retval = UA_Client_readValueAttribute(client, *nodeId, val);

        auto buffer = bufferManager->getBufferBlocking();

        const size_t tupleSize = schema->getSchemaSizeInBytes();

        buffer.setNumberOfTuples(1);
        NES_DEBUG("OPCSource  " << this << ": got buffer ");

        if(retval == UA_STATUSCODE_GOOD && UA_Variant_isScalar(val)) {

            value = *(UA_Int32*)val->data;

            NES_DEBUG("Value is: " << value);

            std::memcpy(buffer.getBuffer(), &value, buffer.getBufferSize());

            return buffer;
        }else{
            NES_ERROR("OPCSOURCE error: Could not retrieve data. Further inspection needed.");

            return std::nullopt;
        }

    } else {
        NES_ERROR("OPCSOURCE: Not connected!");
        return std::nullopt;
    }

}

const std::string OPCSource::toString() const {

    std::stringstream ss;
    ss << "OPC_SOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "URL= " << url << ", ";
    ss << "NODE_INDEX= " << nodeId->namespaceIndex << ". ";

    return ss.str();
}

bool OPCSource::connect() {

    UA_ClientConfig_setDefault(UA_Client_getConfig(client));


    if (!connected) {
        NES_DEBUG("OPCSOURCE was !conncect now connect " << this << ": connected");
        //if (password.empty())
        //{
            retval = UA_Client_connect(client, url.c_str());
            NES_DEBUG("OPCSOURCE connected without user or password");
        //}else{
            //ToDO: Enable username and password connection
            //retval = UA_Client_connect_username(client, url.c_str(), user.c_str(), password.c_str());
        //}
        NES_DEBUG("OPCSOURCE use address " << url);

        if (retval != UA_STATUSCODE_GOOD)
        {
            UA_Client_delete(client);
            connected = false;
            NES_ERROR("OPCSOURCE ERROR with Status Code: " << retval);
            NES_DEBUG("OPCSOURCE " << this << ": set connected false");
        } else{
            connected = true;
        }
    }

    if (connected) {
        NES_DEBUG("OPCSOURCE  " << this << ": connected");
    } else {
        NES_DEBUG("Exception: OPCSOURCE  " << this << ": NOT connected");
    }
    return connected;
}

bool OPCSource::disconnect() {
    NES_DEBUG("OPCSource::disconnect() connected=" << connected);
    if (connected) {

        NES_DEBUG("OPCSOURCE disconnect client");
        UA_Client_disconnect(client);
        NES_DEBUG("OPCSOURCE delete client");
        UA_Client_delete(client);
        connected = false;
    }
    if (!connected) {
        NES_DEBUG("OPCSOURCE  " << this << ": disconnected");
    } else {
        NES_DEBUG("OPCSOURCE  " << this << ": NOT disconnected");
    }
    return !connected;
}

SourceType OPCSource::getType() const { return OPC_SOURCE; }

const std::string&  OPCSource::getUrl() const {
    return url;
}

UA_NodeId* OPCSource::getNodeId() const {
    return nodeId;
}

const std::string& OPCSource::getUser() const {
    return user;
}

const std::string& OPCSource::getPassword() const {
    return password;
}

}// namespace NES
#endif