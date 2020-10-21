#ifdef ENABLE_OPC_BUILD
#include <Sinks/Mediums/OPCSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <cstring>
#include <sstream>
#include <string>

#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>
#include <utility>

#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <open62541/plugin/log_stdout.h>

#include <iostream>
#include <stdlib.h>

namespace NES {

OPCSink::OPCSink(SinkFormatPtr format, std::string url, UA_NodeId nodeId, std::string user, std::string password)
    : SinkMedium(std::move(format), parentPlanId),
      url(url),
      nodeId(nodeId),
      retval(UA_STATUSCODE_GOOD),
      client(UA_Client_new()),
      connected(false),
      user(user),
      password(password) {
    NES_DEBUG("OPCSINK  " << this << ": Init OPC Sink to " << url << " .");
}

OPCSink::~OPCSink() {
    NES_DEBUG("OPCSink::~OPCSink: destructor called");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("OPCSink  " << this << ": Destroy OPC Sink");
    } else {
        NES_ERROR(
            "OPCSink  " << this << ": Destroy OPC Sink failed because it could not be disconnected");
        throw Exception("OPC Sink destruction failed");
    }
    NES_DEBUG("OPCSink  " << this << ": Destroy OPC Sink");
}

bool OPCSink::writeData(TupleBuffer& inputBuffer, WorkerContext&) {
    std::unique_lock lock(writeMutex);
    NES_DEBUG("OPCSINK::writeData()  " << this);
    NES_DEBUG("OPCSINK::writeData url: " << url << ".");
    if (connect()) {

        /* Read current value of attribute, also necessary to get the type information of the node */
        UA_Variant* val = new UA_Variant;
        retval = UA_Client_readValueAttribute(client, nodeId, val);
        if (retval == UA_STATUSCODE_GOOD && UA_Variant_isScalar(val)) {
            NES_DEBUG("OPCSINK::writeData: Node exists, successfully obtained information about current node. ");
            /* Write node attribute with scalar value*/
            retval = UA_Variant_setScalarCopy(val, inputBuffer.getBuffer(), val->type);
        } else if (retval == UA_STATUSCODE_GOOD && UA_Variant_hasArrayType(val, val->type)) {
            NES_DEBUG("OPCSINK::writeData: Node exists, successfully obtained information about current node. ");
            /* Write node attribute with array value*/
            retval = UA_Variant_setArrayCopy(val, inputBuffer.getBuffer(), inputBuffer.getBufferSize(), val->type);
        } else {
            NES_ERROR("OPCSINK::writeData: Node does not exist or is not a scalar.");
            return false;
        }

        if (retval != UA_STATUSCODE_GOOD) {
            NES_ERROR("OPCSINK::writeData: Format of value in input buffer and format of nodeid do not match.");
            return false;
        }

        retval = UA_Client_writeValueAttribute(client, nodeId, val);

        if (retval == UA_STATUSCODE_GOOD) {
            NES_DEBUG("OPCSINK::writeData: Value has been successfully updated ");
        } else {
            NES_ERROR("OPCSINK::writeData: Updating value was not possible.");
            return false;
        }

        UA_delete(val, val->type);

        UA_Variant* var = new UA_Variant;
        UA_Client_readValueAttribute(client, nodeId, var);
        if (retval == UA_STATUSCODE_GOOD && UA_Variant_isScalar(val)) {
            NES_DEBUG("OPCSINK::writeData: New value is: " << *(UA_Int32*) var->data);
        } else {
            NES_ERROR("OPCSINK::writeData: Node does not exist or is not a scalar.");
            return false;
        }
        UA_delete(var, var->type);

        NES_DEBUG("OPCSOURCE::receiveData()  " << this << ": got buffer ");
        NES_DEBUG("OPCSINK::writeData() UA_StatusCode is: " << std::hex << retval);

    } else {
        NES_ERROR("OPCSOURCE::receiveData(): Not connected!");
        return false;
    }
    return true;
}

void OPCSink::setup() {
    NES_DEBUG("OPCSINK::setup");
}

void OPCSink::shutdown() {
    NES_DEBUG("OPCSINK::shutdown");
}

const std::string OPCSink::toString() const {
    char* ident = (char*) UA_malloc(sizeof(char) * nodeId.identifier.string.length + 1);
    memcpy(ident, nodeId.identifier.string.data, nodeId.identifier.string.length);
    ident[nodeId.identifier.string.length] = '\0';
    std::stringstream ss;
    ss << "OPC_SINK(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << "), ";
    ss << "URL= " << this->getUrl() << ", ";
    ss << "NODE_INDEX= " << nodeId.namespaceIndex << ", ";
    ss << "NODE_IDENTIFIER= " << ident << ". ";

    return ss.str();
}

bool OPCSink::connect() {
    UA_ClientConfig_setDefault(UA_Client_getConfig(client));

    if (!connected) {

        NES_DEBUG("OPCSINK::connect(): was !conncect now connect " << this << ": connected");
        retval = UA_Client_connect(client, url.c_str());
        NES_DEBUG("OPCSINK::connect(): connected without user or password");
        NES_DEBUG("OPCSINK::connect(): use address " << url);

        if (retval != UA_STATUSCODE_GOOD) {

            UA_Client_delete(client);
            connected = false;
            NES_ERROR("OPCSINK::connect(): ERROR with Status Code: " << retval << "OPCSINK " << this << ": set connected false");
        } else {

            connected = true;
        }
    }

    if (connected) {
        NES_DEBUG("OPCSINK::connect():  " << this << ": connected");
    } else {
        NES_DEBUG("Exception: OPCSINK::connect():  " << this << ": NOT connected");
    }
    return connected;
}

bool OPCSink::disconnect() {
    NES_DEBUG("OPCSink::disconnect() connected=" << connected);
    if (connected) {

        NES_DEBUG("OPCSINK::disconnect() disconnect client");
        UA_Client_disconnect(client);
        NES_DEBUG("OPCSINK::disconnect() delete client");
        UA_Client_delete(client);
        connected = false;
    }
    if (!connected) {
        NES_DEBUG("OPCSINK::disconnect()  " << this << ": disconnected");
    } else {
        NES_DEBUG("OPCSINK::disconnect()  " << this << ": NOT disconnected");
    }
    return !connected;
}

const std::string OPCSink::getUrl() const {
    return url;
}

UA_NodeId OPCSink::getNodeId() const {
    return nodeId;
}

const std::string OPCSink::getUser() const {
    return user;
}

const std::string OPCSink::getPassword() const {
    return password;
}

SinkMediumTypes OPCSink::getSinkMediumType() {
    return OPC_SINK;
}

std::string OPCSink::toString() {
    return "OPC_SINK";
}

UA_StatusCode OPCSink::getRetval() const {
    return retval;
}

}// namespace NES
#endif