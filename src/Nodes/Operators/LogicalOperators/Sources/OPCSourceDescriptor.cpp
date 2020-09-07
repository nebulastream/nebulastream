#ifdef ENABLE_OPC_BUILD
#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#include <utility>

#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>

namespace NES {

SourceDescriptorPtr OPCSourceDescriptor::create(SchemaPtr schema, const std::string& url, UA_NodeId* nodeId,
                                                std::string user, std::string password) {
    return std::make_shared<OPCSourceDescriptor>(OPCSourceDescriptor(std::move(schema), url, nodeId, std::move(user), std::move(password)));
}

SourceDescriptorPtr OPCSourceDescriptor::create(SchemaPtr schema, std::string streamName, const std::string& url, UA_NodeId* nodeId,
                                                std::string user, std::string password) {
    return std::make_shared<OPCSourceDescriptor>(OPCSourceDescriptor(std::move(schema), std::move(streamName), url, nodeId, std::move(user), std::move(password)));
}

OPCSourceDescriptor::OPCSourceDescriptor(SchemaPtr schema, const std::string& url, UA_NodeId* nodeId, std::string user,
                                         std::string password)
    : SourceDescriptor(std::move(schema)), url(url), nodeId(nodeId), user(std::move(user)), password(std::move(password)) {}

OPCSourceDescriptor::OPCSourceDescriptor(SchemaPtr schema, std::string streamName, const std::string& url, UA_NodeId* nodeId, std::string user,
                                         std::string password)
    : SourceDescriptor(std::move(schema), std::move(streamName)), url(url), nodeId(nodeId), user(std::move(user)), password(std::move(password)) {}

const std::string& OPCSourceDescriptor::getUrl() const {
    return url;
}

UA_NodeId* OPCSourceDescriptor::getNodeId() const {
    return nodeId;
}

const std::string OPCSourceDescriptor::getUser() const {
    return user;
}

const std::string OPCSourceDescriptor::getPassword() const {
    return password;
}

bool OPCSourceDescriptor::equal(SourceDescriptorPtr other) {

    if (!other->instanceOf<OPCSourceDescriptor>())
        return false;
    auto otherOPCSource = other->as<OPCSourceDescriptor>();
    return url == otherOPCSource->getUrl() && UA_NodeId_equal(nodeId, otherOPCSource->getNodeId()) && user == otherOPCSource->getUser() && password == otherOPCSource->getPassword() && getSchema()->equals(other->getSchema());
}

std::string OPCSourceDescriptor::toString() {
    return "OPCSourceDescriptor()";
}

}// namespace NES

#endif