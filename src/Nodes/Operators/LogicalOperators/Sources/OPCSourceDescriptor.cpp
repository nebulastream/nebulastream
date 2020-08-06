#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#include <utility>

#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>

namespace NES {

SourceDescriptorPtr OPCSourceDescriptor::create(SchemaPtr schema, 
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId) {
    return std::make_shared<OPCSourceDescriptor>(OPCSourceDescriptor(std::move(schema), std::move(url), std::move(nsIndex), std::move(nsId)));
}

SourceDescriptorPtr OPCSourceDescriptor::create(SchemaPtr schema,
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId,
        const std::string user,
        const std::string password) {
    return std::make_shared<OPCSourceDescriptor>(OPCSourceDescriptor(std::move(schema), std::move(url), std::move(nsIndex), std::move(nsId), std::move(user), std::move(password)));
}

SourceDescriptorPtr OPCSourceDescriptor::create(SchemaPtr schema, 
        std::string streamName, 
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId) {
    return std::make_shared<OPCSourceDescriptor>(OPCSourceDescriptor(std::move(schema), std::move(streamName), std::move(url), std::move(nsIndex), std::move(nsId)));
}

SourceDescriptorPtr OPCSourceDescriptor::create(SchemaPtr schema, 
        std::string streamName, 
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId,
        const std::string user,
        const std::string password) {
    return std::make_shared<OPCSourceDescriptor>(OPCSourceDescriptor(std::move(schema), std::move(streamName), std::move(url), std::move(nsIndex), std::move(nsId), std::move(user), std::move(password)));
}

OPCSourceDescriptor::OPCSourceDescriptor(SchemaPtr schema, 
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId) 
    : SourceDescriptor(std::move(schema)), url(std::move(url)), nsIndex(std::move(nsIndex)), nsId(std::move(nsId)) {}

OPCSourceDescriptor::OPCSourceDescriptor(SchemaPtr schema, 
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId,
        const std::string user,
        const std::string password)
    : SourceDescriptor(std::move(schema)), url(std::move(url)), nsIndex(std::move(nsIndex)), nsId(std::move(nsId)), user(std::move(user)), password(std::move(password)) {}

OPCSourceDescriptor::OPCSourceDescriptor(SchemaPtr schema, 
        std::string streamName, 
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId)
    : SourceDescriptor(std::move(schema), std::move(streamName)), url(std::move(url)), nsIndex(std::move(nsIndex)), nsId(std::move(nsId)) {}

OPCSourceDescriptor::OPCSourceDescriptor(SchemaPtr schema, 
        std::string streamName, 
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId,
        const std::string user,
        const std::string password)
    : SourceDescriptor(std::move(schema), std::move(streamName)), url(std::move(url)), nsIndex(std::move(nsIndex)), nsId(std::move(nsId)), user(std::move(user)), password(std::move(password)) {}

const std::string& OPCSourceDescriptor::getUrl() const {
    return url;
}

UA_UInt16 OPCSourceDescriptor::getNsIndex() const {
    return nsIndex;
}

char OPCSourceDescriptor::getNsId() const {
    return *nsId;
}

const std::string& OPCSourceDescriptor::getUser() const {
    return user;
}

const std::string& OPCSourceDescriptor::getPassword() const {
    return password;
}


bool OPCSourceDescriptor::equal(SourceDescriptorPtr other) {

    if (!other->instanceOf<OPCSourceDescriptor>())
        return false;
    auto otherOPCSource = other->as<OPCSourceDescriptor>();
    return url == otherOPCSource->getUrl() && nsIndex == otherOPCSource->getNsIndex() && *nsId == otherOPCSource->getNsId() && user == otherOPCSource->getUser() && password == otherOPCSource->getPassword() && getSchema()->equals(other->getSchema());
}

std::string OPCSourceDescriptor::toString() {
    return "OPCSourceDescriptor()";
}

}// namespace NES