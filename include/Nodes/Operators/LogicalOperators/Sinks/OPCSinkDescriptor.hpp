#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_ZMQSINKDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_ZMQSINKDESCRIPTOR_HPP_

#ifdef ENABLE_OPC_BUILD

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical opc sink
 */
class OPCSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief Creates the OPC sink description
     * @param url: server url used to connect to OPC server
     * @param nodeId: id of node to write data to
     * @param user: user name for server
     * @param password: password for server
     * @return descriptor for OPC sink
     */
    static SinkDescriptorPtr create(const std::string& url,
                                    UA_NodeId* nodeId,
                                    std::string user,
                                    std::string password);

    /**
     * @brief Get the OPC server url where the data is to be written to
     */
    const std::string& getUrl() const;

    /**
     * @brief get node id of node to be written to
     */
    UA_NodeId* getNodeId() const;

    /**
     * @brief get user name for opc server
     */
    const std::string getUser() const;

    /**
     * @brief get password for opc server
     */
    const std::string getPassword() const;

    std::string toString() override;
    bool equal(SinkDescriptorPtr other) override;

  private:
    explicit OPCSinkDescriptor(const std::string& url, UA_NodeId* nodeId,
                               std::string user, std::string password);

    const std::string& url;
    UA_NodeId* nodeId;
    const std::string user;
    const std::string password;
};

typedef std::shared_ptr<OPCSinkDescriptor> OPCSinkDescriptorPtr;

}// namespace NES
#endif
#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_ZMQSINKDESCRIPTOR_HPP_
