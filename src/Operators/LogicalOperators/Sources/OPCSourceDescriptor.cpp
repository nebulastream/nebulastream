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

#ifdef ENABLE_OPC_BUILD
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#include <utility>

#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>

namespace NES {

SourceDescriptorPtr OPCSourceDescriptor::create(SchemaPtr schema, std::string url, UA_NodeId nodeId, std::string user,
                                                std::string password) {
    return std::make_shared<OPCSourceDescriptor>(
        OPCSourceDescriptor(std::move(schema), std::move(url), std::move(nodeId), std::move(user), std::move(password)));
}

SourceDescriptorPtr OPCSourceDescriptor::create(SchemaPtr schema, std::string streamName, std::string url, UA_NodeId nodeId,
                                                std::string user, std::string password) {
    return std::make_shared<OPCSourceDescriptor>(OPCSourceDescriptor(std::move(schema), std::move(streamName), std::move(url),
                                                                     std::move(nodeId), std::move(user), std::move(password)));
}

OPCSourceDescriptor::OPCSourceDescriptor(SchemaPtr schema, std::string url, UA_NodeId nodeId, std::string user,
                                         std::string password)
    : SourceDescriptor(std::move(schema)), url(std::move(url)), nodeId(std::move(nodeId)), user(std::move(user)),
      password(std::move(password)) {}

OPCSourceDescriptor::OPCSourceDescriptor(SchemaPtr schema, std::string streamName, std::string url, UA_NodeId nodeId,
                                         std::string user, std::string password)
    : SourceDescriptor(std::move(schema), std::move(streamName)), url(std::move(url)), nodeId(std::move(nodeId)),
      user(std::move(user)), password(std::move(password)) {}

const std::string OPCSourceDescriptor::getUrl() const { return url; }

UA_NodeId OPCSourceDescriptor::getNodeId() const { return nodeId; }

const std::string OPCSourceDescriptor::getUser() const { return user; }

const std::string OPCSourceDescriptor::getPassword() const { return password; }

bool OPCSourceDescriptor::equal(SourceDescriptorPtr other) {

    if (!other->instanceOf<OPCSourceDescriptor>())
        return false;
    auto otherOPCSource = other->as<OPCSourceDescriptor>();
    NES_DEBUG("URL= " << url << " == " << otherOPCSource->getUrl());
    char* otherOperatorIdent = (char*) UA_malloc(sizeof(char) * otherOPCSource->getNodeId().identifier.string.length + 1);
    memcpy(otherOperatorIdent, otherOPCSource->getNodeId().identifier.string.data,
           otherOPCSource->getNodeId().identifier.string.length);
    otherOperatorIdent[otherOPCSource->getNodeId().identifier.string.length] = '\0';
    char* ident = (char*) UA_malloc(sizeof(char) * nodeId.identifier.string.length + 1);
    memcpy(ident, nodeId.identifier.string.data, nodeId.identifier.string.length);
    ident[nodeId.identifier.string.length] = '\0';
    return url == otherOPCSource->getUrl() && !strcmp(ident, otherOperatorIdent)
        && nodeId.namespaceIndex == otherOPCSource->getNodeId().namespaceIndex
        && nodeId.identifierType == otherOPCSource->getNodeId().identifierType && user == otherOPCSource->getUser()
        && password == otherOPCSource->getPassword();
}

std::string OPCSourceDescriptor::toString() { return "OPCSourceDescriptor()"; }

}// namespace NES

#endif