#ifdef ENABLE_OPC_BUILD
#include <Sinks/Mediums/OPCSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <cstring>
#include <sstream>
#include <string>

#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>

namespace NES {

OPCSink::OPCSink(SinkFormatPtr format, const std::string url, UA_NodeId* nodeId, const std::string user, const std::string password)
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
    connect();
    if (!connected) {
        NES_DEBUG(
            "OPCSink  " << this << ": cannot write buffer " << inputBuffer << " because connection to server was not established.");
        throw Exception("OPCSINK::Write to opc sink failed");
    }

    if (!inputBuffer.isValid()) {
        NES_ERROR("OPCSink::writeData input buffer invalid");
        return false;
    }

    UA_Variant* val = UA_Variant_new();
    UA_WriteRequest wReq;
    UA_WriteRequest_init(&wReq);
    std::memcpy(val->data, inputBuffer.getBuffer(), inputBuffer.getBufferSize());
    char* ident = (char*)UA_malloc(sizeof(char)*nodeId->identifier.string.length+1);
    memcpy(ident, nodeId->identifier.string.data, nodeId->identifier.string.length);
    ident[nodeId->identifier.string.length] = '\0';

    NES_DEBUG("OPCSINK:: Try writing " << val << " to ns=" << nodeId->namespaceIndex << ";s=" << ident <<".");

    wReq.nodesToWrite = UA_WriteValue_new();
    wReq.nodesToWriteSize = 1;
    wReq.nodesToWrite[0].nodeId = UA_NODEID_STRING_ALLOC(nodeId->namespaceIndex, ident);
    wReq.nodesToWrite[0].attributeId = UA_ATTRIBUTEID_VALUE;
    wReq.nodesToWrite[0].value.hasValue = true;
    wReq.nodesToWrite[0].value.value.type = &UA_TYPES[UA_TYPES_INT32];
    wReq.nodesToWrite[0].value.value.storageType = UA_VARIANT_DATA_NODELETE; /* do not free the integer on deletion */
    wReq.nodesToWrite[0].value.value.data = &val;
    UA_WriteResponse wResp = UA_Client_Service_write(client, wReq);
    if(wResp.responseHeader.serviceResult == UA_STATUSCODE_GOOD){
        NES_DEBUG("OPCSINK:: Wrote " << val << " to ns=" << nodeId->namespaceIndex << ";s=" << ident <<".");
    } else{
        NES_DEBUG("OPCSINK::Could not write " << val << " to ns=" << nodeId->namespaceIndex << ";s=" << ident <<".");
    }
    UA_WriteRequest_clear(&wReq);
    UA_WriteResponse_clear(&wResp);

    return true;
}
void OPCSink::setup() {
}
void OPCSink::shutdown() {
}
const std::string OPCSink::toString() const {
    char* ident = (char*)UA_malloc(sizeof(char)*nodeId->identifier.string.length+1);
    memcpy(ident, nodeId->identifier.string.data, nodeId->identifier.string.length);
    ident[nodeId->identifier.string.length] = '\0';
    std::stringstream ss;
    ss << "OPC_SINK(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << "), ";
    ss << "URL= " << url << ", ";
    ss << "NODE_INDEX= " << nodeId->namespaceIndex << ", ";
    ss << "NODE_IDENTIFIER= " << ident << ". ";

    return ss.str();
}
const std::string OPCSink::getUrl() const {
    return url;
}
UA_NodeId* OPCSink::getNodeId() const {
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

}
// namespace NES
#endif