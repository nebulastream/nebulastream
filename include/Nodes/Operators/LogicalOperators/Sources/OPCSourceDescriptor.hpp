#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_OPCSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_OPCSOURCEDESCRIPTOR_HPP_

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
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId);

    static SourceDescriptorPtr create(SchemaPtr schema, 
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId,
        const std::string user,
        const std::string password);
    
    static SourceDescriptorPtr create(SchemaPtr schema, 
        std::string streamName, 
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId);

    static SourceDescriptorPtr create(SchemaPtr schema, 
        std::string streamName, 
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId,
        const std::string user,
        const std::string password);

    const std::string& getUrl() const;

    UA_UInt16 getNsIndex() const;

    char getNsId() const;

    const std::string& getUser() const;

    const std::string& getPassword() const;
    

    bool equal(SourceDescriptorPtr other) override;
    std::string toString() override;

  private:
    explicit OPCSourceDescriptor(SchemaPtr schema,
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId);

    explicit OPCSourceDescriptor(SchemaPtr schema, 
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId,
        const std::string user,
        const std::string password);

    explicit OPCSourceDescriptor(SchemaPtr schema, 
        std::string streamName, 
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId);

    explicit OPCSourceDescriptor(SchemaPtr schema, 
        std::string streamName, 
        const std::string url,
        UA_UInt16 nsIndex,
        char *nsId,
        const std::string user,
        const std::string password);

    std::string url;
    UA_UInt16 nsIndex;
    char *nsId;
    std::string user;
    std::string password;
};

typedef std::shared_ptr<OPCSourceDescriptor> OPCSourceDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_