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
#include <Configurations/ConfigOptions/SourceConfigurations/OPCSourceConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {
std::shared_ptr<OPCSourceConfig> OPCSourceConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<OPCSourceConfig>(OPCSourceConfig(std::move(sourceConfigMap)));
}

std::shared_ptr<OPCSourceConfig> OPCSourceConfig::create() {
    return std::make_shared<OPCSourceConfig>(OPCSourceConfig());
}

OPCSourceConfig::OPCSourceConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceConfig(std::move(sourceConfigMap)),
      namespaceIndex(ConfigOption<uint32_t>::create("namespaceIndex", 1, "namespaceIndex for node, needed for: OPCSource")),
      nodeIdentifier(ConfigOption<std::string>::create("nodeIdentifier", "the.answer", "node identifier, needed for: OPCSource")),
      userName(ConfigOption<std::string>::create("userName",
                                                 "testUser",
                                                 "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      password(ConfigOption<std::string>::create("password", "", "password, needed for: OPCSource")) {
    NES_INFO("OPCSourceConfig: Init source config object with values from sourceConfigMap.");
    if (sourceConfigMap.find("namespaceIndex") != sourceConfigMap.end()) {
        namespaceIndex->setValue(std::stoi(sourceConfigMap.find("namespaceIndex")->second));
    }
    if (sourceConfigMap.find("nodeIdentifier") != sourceConfigMap.end()) {
        nodeIdentifier->setValue(sourceConfigMap.find("nodeIdentifier")->second);
    }
    if (sourceConfigMap.find("userName") != sourceConfigMap.end()) {
        userName->setValue(sourceConfigMap.find("userName")->second);
    }
    if (sourceConfigMap.find("password") != sourceConfigMap.end()) {
        password->setValue(sourceConfigMap.find("password")->second);
    }
}

OPCSourceConfig::OPCSourceConfig()
    : SourceConfig(),
      namespaceIndex(ConfigOption<uint32_t>::create("namespaceIndex", 1, "namespaceIndex for node, needed for: OPCSource")),
      nodeIdentifier(ConfigOption<std::string>::create("nodeIdentifier", "the.answer", "node identifier, needed for: OPCSource")),
      userName(ConfigOption<std::string>::create("userName",
                                                 "testUser",
                                                 "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      password(ConfigOption<std::string>::create("password", "", "password, needed for: OPCSource")) {
    NES_INFO("OPCSourceConfig: Init source config object with default values.");

}

void OPCSourceConfig::resetSourceOptions() {
    setNamespaceIndex(namespaceIndex->getDefaultValue());
    setNodeIdentifier(nodeIdentifier->getDefaultValue());
    setUserName(userName->getDefaultValue());
    setPassword(password->getDefaultValue());
    SourceConfig::resetSourceOptions();
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

}// namespace NES