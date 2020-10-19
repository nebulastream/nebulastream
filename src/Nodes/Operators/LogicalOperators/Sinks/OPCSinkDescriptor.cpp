#ifdef ENABLE_OPC_BUILD
#include <Nodes/Operators/LogicalOperators/Sinks/OPCSinkDescriptor.hpp>
#include <open62541/client_config_default.h>

namespace NES {

    OPCSinkDescriptor::OPCSinkDescriptor(const std::string& url,
                                     UA_NodeId* nodeId,
                                     std::string user,
                                     std::string password)
        : url(url), nodeId(nodeId), user(std::move(user)), password(std::move(password)) {

        NES_DEBUG("OPCSINKDESCRIPTOR  " << this << ": Init OPC Sink descriptor.");
    }

    const std::string& OPCSinkDescriptor::getUrl() const {
        return url;
    }

    UA_NodeId* OPCSinkDescriptor::getNodeId() const {
        return nodeId;
    }

    const std::string OPCSinkDescriptor::getUser() const {
        return user;
    }

    const std::string OPCSinkDescriptor::getPassword() const {
        return password;
    }

    SinkDescriptorPtr OPCSinkDescriptor::create(const std::string& url, UA_NodeId* nodeId,
                                                std::string user, std::string password) {
        return std::make_shared<OPCSinkDescriptor>(OPCSinkDescriptor(url, nodeId, std::move(user), std::move(password)));
    }

    std::string OPCSinkDescriptor::toString() {
        return "OPCSinkDescriptor()";
    }

    bool OPCSinkDescriptor::equal(SinkDescriptorPtr other) {
        if (!other->instanceOf<OPCSinkDescriptor>())
            return false;
        auto otherSinkDescriptor = other->as<OPCSinkDescriptor>();
        return url == otherSinkDescriptor->getUrl() && UA_NodeId_equal(nodeId, otherSinkDescriptor->getNodeId()) && user == otherSinkDescriptor->getUser() && password == otherSinkDescriptor->getPassword();
    }

}// namespace NES
#endif
