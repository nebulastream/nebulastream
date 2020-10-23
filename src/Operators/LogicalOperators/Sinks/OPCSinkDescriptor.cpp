#ifdef ENABLE_OPC_BUILD
#include <Operators/LogicalOperators/Sinks/OPCSinkDescriptor.hpp>
#include <open62541/client_config_default.h>

namespace NES {

    OPCSinkDescriptor::OPCSinkDescriptor(std::string url,
                                     UA_NodeId nodeId,
                                     std::string user,
                                     std::string password)
        : url(std::move(url)), nodeId(std::move(nodeId)), user(std::move(user)), password(std::move(password)) {

        NES_DEBUG("OPCSINKDESCRIPTOR  " << this << ": Init OPC Sink descriptor.");
    }

    const std::string OPCSinkDescriptor::getUrl() const {
        return url;
    }

    UA_NodeId OPCSinkDescriptor::getNodeId() const {
        return nodeId;
    }

    const std::string OPCSinkDescriptor::getUser() const {
        return user;
    }

    const std::string OPCSinkDescriptor::getPassword() const {
        return password;
    }

    SinkDescriptorPtr OPCSinkDescriptor::create(std::string url, UA_NodeId nodeId,
                                                std::string user, std::string password) {
        return std::make_shared<OPCSinkDescriptor>(OPCSinkDescriptor(std::move(url), std::move(nodeId), std::move(user), std::move(password)));
    }

    std::string OPCSinkDescriptor::toString() {
        return "OPCSinkDescriptor()";
    }

    bool OPCSinkDescriptor::equal(SinkDescriptorPtr other) {
        if (!other->instanceOf<OPCSinkDescriptor>()){
            NES_DEBUG("Instance of "<< other->instanceOf<OPCSinkDescriptor>());
            return false;}
        auto otherSinkDescriptor = other->as<OPCSinkDescriptor>();
        char* newIdent = (char*) UA_malloc(sizeof(char) * nodeId.identifier.string.length + 1);
        memcpy(newIdent, nodeId.identifier.string.data, nodeId.identifier.string.length);
        newIdent[nodeId.identifier.string.length] = '\0';
        char* otherSinkIdent = (char*) UA_malloc(sizeof(char) * otherSinkDescriptor->getNodeId().identifier.string.length + 1);
        memcpy(otherSinkIdent, otherSinkDescriptor->getNodeId().identifier.string.data, otherSinkDescriptor->getNodeId().identifier.string.length);
        otherSinkIdent[otherSinkDescriptor->getNodeId().identifier.string.length] = '\0';
        return url == otherSinkDescriptor->getUrl() && !strcmp(newIdent, otherSinkIdent) && nodeId.namespaceIndex == otherSinkDescriptor->getNodeId().namespaceIndex && nodeId.identifierType == otherSinkDescriptor->getNodeId().identifierType && user == otherSinkDescriptor->getUser() && password == otherSinkDescriptor->getPassword();
    }

}// namespace NES
#endif
