#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_OPCSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_OPCSOURCEDESCRIPTOR_HPP_

#ifdef ENABLE_KAFKA_BUILD

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>


#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>


namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical zmq source
 */
class OPCSourceDescriptor : public SourceDescriptor {

  public:

    static SourceDescriptorPtr create(SchemaPtr schema,
                                      const std::string & url,
                                      UA_NodeId *nodeId,
                                      const std::string& user,
                                      const std::string& password);

    static SourceDescriptorPtr create(SchemaPtr schema, 
        std::string streamName,
                                      const std::string & url,
        UA_NodeId *nodeId,
                                      const std::string& user,
                                      const std::string& password);

    const std::string& getUrl() const;

    UA_NodeId* getNodeId() const;

    const std::string& getUser() const;

    const std::string& getPassword() const;
    

    bool equal(SourceDescriptorPtr other) override;
    std::string toString() override;

  private:

    explicit OPCSourceDescriptor(SchemaPtr schema,
                                 const std::string & url,
                                 UA_NodeId *nodeId,
                                 const std::string& user,
                                 const std::string& password);

    explicit OPCSourceDescriptor(SchemaPtr schema, 
        std::string streamName,
                                 const std::string & url,
        UA_NodeId *nodeId,
                                 const std::string& user,
                                 const std::string& password);

    const std::string & url;
    UA_NodeId *nodeId;
    const std::string& user;
    const std::string& password;
};

typedef std::shared_ptr<OPCSourceDescriptor> OPCSourceDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_