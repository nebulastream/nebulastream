#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_OPCSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_OPCSOURCEDESCRIPTOR_HPP_

#ifdef ENABLE_OPC_BUILD

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical opc source
 */
class OPCSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema,
                                      const std::string& url,
                                      UA_NodeId* nodeId,
                                      std::string user,
                                      std::string password);

    static SourceDescriptorPtr create(SchemaPtr schema,
                                      std::string streamName,
                                      const std::string& url,
                                      UA_NodeId* nodeId,
                                      std::string user,
                                      std::string password);

    /**
     * @brief get OPC server url
     */
    const std::string& getUrl() const;

    /**
     * @brief get desired node id
     */
    UA_NodeId* getNodeId() const;

    /**
     * @brief get user name
     */
    const std::string getUser() const;

    /**
     * @brief get password
     */
    const std::string getPassword() const;

    bool equal(SourceDescriptorPtr other) override;

    std::string toString() override;

  private:
    explicit OPCSourceDescriptor(SchemaPtr schema, const std::string& url, UA_NodeId* nodeId,
                                 std::string user, std::string password);

    explicit OPCSourceDescriptor(SchemaPtr schema, std::string streamName, const std::string& url,
                                 UA_NodeId* nodeId, std::string user, std::string password);

    const std::string& url;
    UA_NodeId* nodeId;
    const std::string user;
    const std::string password;
};

typedef std::shared_ptr<OPCSourceDescriptor> OPCSourceDescriptorPtr;

}// namespace NES

#endif
#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_