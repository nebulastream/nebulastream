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

#include <Configurations/ConfigOption.hpp>
#include <Configurations/Sources/OPCSourceConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

std::shared_ptr<OPCSourceConfig> OPCSourceConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<OPCSourceConfig>(OPCSourceConfig(std::move(sourceConfigMap)));
}

std::shared_ptr<OPCSourceConfig> OPCSourceConfig::create() { return std::make_shared<OPCSourceConfig>(OPCSourceConfig()); }

OPCSourceConfig::OPCSourceConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceConfig(sourceConfigMap, OPC_SOURCE_CONFIG),
      namespaceIndex(
          ConfigOption<uint32_t>::create(NAME_SPACE_INDEX_CONFIG, 1, "namespaceIndex for node, needed for: OPCSource")),
      nodeIdentifier(ConfigOption<std::string>::create(NODE_IDENTIFIER_CONFIG, "", "node identifier, needed for: OPCSource")),
      userName(ConfigOption<std::string>::create(USER_NAME_CONFIG,
                                                 "",
                                                 "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      password(ConfigOption<std::string>::create(PASSWORD_CONFIG, "", "password, needed for: OPCSource")) {
    NES_INFO("OPCSourceConfig: Init source config object with values from sourceConfigMap.");
    if (sourceConfigMap.find(OPC_SOURCE_NAME_SPACE_INDEX_CONFIG) != sourceConfigMap.end()) {
        namespaceIndex->setValue(std::stoi(sourceConfigMap.find(OPC_SOURCE_NAME_SPACE_INDEX_CONFIG)->second));
    }
    if (sourceConfigMap.find(OPC_SOURCE_NODE_IDENTIFIER_CONFIG) != sourceConfigMap.end()) {
        nodeIdentifier->setValue(sourceConfigMap.find(OPC_SOURCE_NODE_IDENTIFIER_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("OPCSourceConfig:: no nodeIdentifier defined! Please define a nodeIdentifier.");
    }
    if (sourceConfigMap.find(OPC_SOURCE_USERNAME_CONFIG) != sourceConfigMap.end()) {
        userName->setValue(sourceConfigMap.find(OPC_SOURCE_USERNAME_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("OPCSourceConfig:: no userName defined! Please define a userName.");
    }
    if (sourceConfigMap.find(OPC_SOURCE_PASSWORD_CONFIG) != sourceConfigMap.end()) {
        password->setValue(sourceConfigMap.find(OPC_SOURCE_PASSWORD_CONFIG)->second);
    }
}

OPCSourceConfig::OPCSourceConfig()
    : SourceConfig(OPC_SOURCE_CONFIG),
      namespaceIndex(
          ConfigOption<uint32_t>::create(NAME_SPACE_INDEX_CONFIG, 1, "namespaceIndex for node, needed for: OPCSource")),
      nodeIdentifier(
          ConfigOption<std::string>::create(NODE_IDENTIFIER_CONFIG, "the.answer", "node identifier, needed for: OPCSource")),
      userName(ConfigOption<std::string>::create(USER_NAME_CONFIG,
                                                 "testUser",
                                                 "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      password(ConfigOption<std::string>::create(PASSWORD_CONFIG, "", "password, needed for: OPCSource")) {
    NES_INFO("OPCSourceConfig: Init source config object with default values.");
}

void OPCSourceConfig::resetSourceOptions() {
    setNamespaceIndex(namespaceIndex->getDefaultValue());
    setNodeIdentifier(nodeIdentifier->getDefaultValue());
    setUserName(userName->getDefaultValue());
    setPassword(password->getDefaultValue());
    SourceConfig::resetSourceOptions(OPC_SOURCE_CONFIG);
}

std::string OPCSourceConfig::toString() {
    std::stringstream ss;
    ss << namespaceIndex->toStringNameCurrentValue();
    ss << nodeIdentifier->toStringNameCurrentValue();
    ss << userName->toStringNameCurrentValue();
    ss << password->toStringNameCurrentValue();
    ss << SourceConfig::toString();
    return ss.str();
}

bool OPCSourceConfig::equal(const SourceConfigPtr& other) {
    if (!other->instanceOf<OPCSourceConfig>()) {
        return false;
    }
    auto otherSourceConfig = other->as<OPCSourceConfig>();
    return SourceConfig::equal(other) && namespaceIndex->getValue() == otherSourceConfig->namespaceIndex->getValue()
        && nodeIdentifier->getValue() == otherSourceConfig->nodeIdentifier->getValue()
        && userName->getValue() == otherSourceConfig->userName->getValue()
        && password->getValue() == otherSourceConfig->password->getValue();
}

IntConfigOption OPCSourceConfig::getNamespaceIndex() const { return namespaceIndex; }

StringConfigOption OPCSourceConfig::getNodeIdentifier() const { return nodeIdentifier; }

StringConfigOption OPCSourceConfig::getUserName() const { return userName; }

StringConfigOption OPCSourceConfig::getPassword() const { return password; }

void OPCSourceConfig::setNamespaceIndex(uint32_t namespaceIndexValue) { namespaceIndex->setValue(namespaceIndexValue); }

void OPCSourceConfig::setNodeIdentifier(std::string nodeIdentifierValue) {
    nodeIdentifier->setValue(std::move(nodeIdentifierValue));
}

void OPCSourceConfig::setUserName(std::string userNameValue) { userName->setValue(userNameValue); }

void OPCSourceConfig::setPassword(std::string passwordValue) { password->setValue(std::move(passwordValue)); }

}// namespace Configurations
}// namespace NES