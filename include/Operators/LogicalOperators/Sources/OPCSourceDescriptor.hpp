/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_OPCSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_OPCSOURCEDESCRIPTOR_HPP_

#ifdef ENABLE_OPC_BUILD

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical opc source
 */
class OPCSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, std::string url, UA_NodeId nodeId, std::string user,
                                      std::string password);

    static SourceDescriptorPtr create(SchemaPtr schema, std::string streamName, std::string url, UA_NodeId nodeId,
                                      std::string user, std::string password);

    /**
     * @brief get OPC server url
     */
    const std::string getUrl() const;

    /**
     * @brief get desired node id
     */
    UA_NodeId getNodeId() const;

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
    explicit OPCSourceDescriptor(SchemaPtr schema, std::string url, UA_NodeId nodeId, std::string user, std::string password);

    explicit OPCSourceDescriptor(SchemaPtr schema, std::string streamName, std::string url, UA_NodeId nodeId, std::string user,
                                 std::string password);

    const std::string url;
    UA_NodeId nodeId;
    const std::string user;
    const std::string password;
};

typedef std::shared_ptr<OPCSourceDescriptor> OPCSourceDescriptorPtr;

}// namespace NES

#endif
#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_