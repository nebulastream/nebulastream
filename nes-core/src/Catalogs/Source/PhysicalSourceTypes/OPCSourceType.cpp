/*
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

#include <Catalogs/Source/PhysicalSourceTypes/OPCSourceType.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

OPCSourceTypePtr OPCSourceType::create(Yaml::Node yamlConfig) {
    return std::make_shared<OPCSourceType>(OPCSourceType(std::move(yamlConfig)));
}

OPCSourceTypePtr OPCSourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<OPCSourceType>(OPCSourceType(std::move(sourceConfigMap)));
}

OPCSourceTypePtr OPCSourceType::create() { return std::make_shared<OPCSourceType>(OPCSourceType()); }

OPCSourceType::OPCSourceType(std::map<std::string, std::string> sourceConfigMap) : OPCSourceType() {
    NES_INFO("OPCSourceType: Init default OPC source config object with values from command line args.");
    if (sourceConfigMap.find(Configurations::OPC_SOURCE_CONFIG) != sourceConfigMap.end()) {
        namespaceIndex->setValue(std::stoi(sourceConfigMap.find(Configurations::OPC_SOURCE_CONFIG)->second));
    }
    if (sourceConfigMap.find(Configurations::NAME_SPACE_INDEX_CONFIG) != sourceConfigMap.end()) {
        nodeIdentifier->setValue(sourceConfigMap.find(Configurations::NAME_SPACE_INDEX_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("OPCSourceConfig:: no nodeIdentifier defined! Please define a nodeIdentifier.");
    }
    if (sourceConfigMap.find(Configurations::USER_NAME_CONFIG) != sourceConfigMap.end()) {
        userName->setValue(sourceConfigMap.find(Configurations::USER_NAME_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("OPCSourceConfig:: no userName defined! Please define a userName.");
    }
    if (sourceConfigMap.find(Configurations::PASSWORD_CONFIG) != sourceConfigMap.end()) {
        password->setValue(sourceConfigMap.find(Configurations::PASSWORD_CONFIG)->second);
    }
}

OPCSourceType::OPCSourceType(Yaml::Node yamlConfig) : OPCSourceType() {
    NES_INFO("OPCSourceType: Init default OPC source config object with values from YAML file.");
    if (!yamlConfig[Configurations::NAME_SPACE_INDEX_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::NAME_SPACE_INDEX_CONFIG].As<std::string>() != "\n") {
        namespaceIndex->setValue(yamlConfig[Configurations::NAME_SPACE_INDEX_CONFIG].As<std::uint32_t>());
    }
    if (!yamlConfig[Configurations::NODE_IDENTIFIER_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::NODE_IDENTIFIER_CONFIG].As<std::string>() != "\n") {
        nodeIdentifier->setValue(yamlConfig[Configurations::NODE_IDENTIFIER_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR("OPCSourceType:: no nodeIdentifier defined! Please define a nodeIdentifier.");
    }
    if (!yamlConfig[Configurations::USER_NAME_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::USER_NAME_CONFIG].As<std::string>() != "\n") {
        userName->setValue(yamlConfig[Configurations::USER_NAME_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR("OPCSourceType:: no userName defined! Please define a userName.");
    }
    if (!yamlConfig[Configurations::PASSWORD_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::PASSWORD_CONFIG].As<std::string>() != "\n") {
        password->setValue(yamlConfig[Configurations::PASSWORD_CONFIG].As<std::string>());
    }
}

OPCSourceType::OPCSourceType()
    : PhysicalSourceType(OPC_SOURCE),
      namespaceIndex(Configurations::ConfigurationOption<uint32_t>::create(Configurations::NAME_SPACE_INDEX_CONFIG,
                                                                           1,
                                                                           "namespaceIndex for node, needed for: OPCSource")),
      nodeIdentifier(Configurations::ConfigurationOption<std::string>::create(Configurations::NODE_IDENTIFIER_CONFIG,
                                                                              "the.answer",
                                                                              "node identifier, needed for: OPCSource")),
      userName(Configurations::ConfigurationOption<std::string>::create(
          Configurations::USER_NAME_CONFIG,
          "testUser",
          "userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource")),
      password(Configurations::ConfigurationOption<std::string>::create(Configurations::PASSWORD_CONFIG,
                                                                        "",
                                                                        "password, needed for: OPCSource")) {
    NES_INFO("OPCSourceType: Init source config object with default values.");
}

std::string OPCSourceType::toString() {
    std::stringstream ss;
    ss << Configurations::NAME_SPACE_INDEX_CONFIG + ":" + namespaceIndex->toStringNameCurrentValue();
    ss << Configurations::NODE_IDENTIFIER_CONFIG + ":" + nodeIdentifier->toStringNameCurrentValue();
    ss << Configurations::USER_NAME_CONFIG + ":" + userName->toStringNameCurrentValue();
    ss << Configurations::PASSWORD_CONFIG + ":" + password->toStringNameCurrentValue();
    return ss.str();
}

bool OPCSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<OPCSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<OPCSourceType>();
    return namespaceIndex->getValue() == otherSourceConfig->namespaceIndex->getValue()
        && nodeIdentifier->getValue() == otherSourceConfig->nodeIdentifier->getValue()
        && userName->getValue() == otherSourceConfig->userName->getValue()
        && password->getValue() == otherSourceConfig->password->getValue();
}

Configurations::IntConfigOption OPCSourceType::getNamespaceIndex() const { return namespaceIndex; }

Configurations::StringConfigOption OPCSourceType::getNodeIdentifier() const { return nodeIdentifier; }

Configurations::StringConfigOption OPCSourceType::getUserName() const { return userName; }

Configurations::StringConfigOption OPCSourceType::getPassword() const { return password; }

void OPCSourceType::setNamespaceIndex(uint32_t namespaceIndexValue) { namespaceIndex->setValue(namespaceIndexValue); }

void OPCSourceType::setNodeIdentifier(std::string nodeIdentifierValue) {
    nodeIdentifier->setValue(std::move(nodeIdentifierValue));
}

void OPCSourceType::setUserName(std::string userNameValue) { userName->setValue(userNameValue); }

void OPCSourceType::setPassword(std::string passwordValue) { password->setValue(std::move(passwordValue)); }

void OPCSourceType::reset() {
    setNamespaceIndex(namespaceIndex->getDefaultValue());
    setNodeIdentifier(nodeIdentifier->getDefaultValue());
    setUserName(userName->getDefaultValue());
    setPassword(password->getDefaultValue());
}

}// namespace NES