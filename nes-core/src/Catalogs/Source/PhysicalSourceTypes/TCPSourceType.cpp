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

#include "Catalogs/Source/PhysicalSourceTypes/TCPSourceType.hpp"

namespace NES {

TCPSourceTypePtr TCPSourceType::create(Yaml::Node yamlConfig) {
    return std::make_shared<TCPSourceType>(TCPSourceType(std::move(yamlConfig)));
}

TCPSourceTypePtr TCPSourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<TCPSourceType>(TCPSourceType(std::move(sourceConfigMap)));
}

TCPSourceTypePtr TCPSourceType::create() { return std::make_shared<TCPSourceType>(TCPSourceType()); }

TCPSourceType::TCPSourceType(std::map<std::string, std::string> sourceConfigMap) : TCPSourceType() {
    NES_INFO("TCPSourceType: Init default TCP source config object with values from command line args.");

    if (sourceConfigMap.find(Configurations::URL_CONFIG) != sourceConfigMap.end()) {
        url->setValue(sourceConfigMap.find(Configurations::URL_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("TCPSourceType:: no Url defined! Please define a Url.");
    }
}

TCPSourceType::TCPSourceType(Yaml::Node yamlConfig) : TCPSourceType() {
    NES_INFO("TCPSourceType: Init default TCP source config object with values from YAML file.");

    if (!yamlConfig[Configurations::URL_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::URL_CONFIG].As<std::string>() != "\n") {
        url->setValue(yamlConfig[Configurations::URL_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR("TCPSourceType:: no Url defined! Please define a Url.");
    }
}

TCPSourceType::TCPSourceType()
    : PhysicalSourceType(TCP_SOURCE), url(Configurations::ConfigurationOption<std::string>::create(
                                           Configurations::URL_CONFIG,
                                           "tcp://127.0.0.1:3000",
                                           "url to connect to")) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

std::string TCPSourceType::toString() {
    std::stringstream ss;
    ss << "TCPSourceType => {\n";
    ss << Configurations::URL_CONFIG + ":" + url->toStringNameCurrentValue();
    ss << "\n}";
    return ss.str();
}

bool TCPSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<TCPSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<TCPSourceType>();
    return url->getValue() == otherSourceConfig->url->getValue();
}

Configurations::StringConfigOption TCPSourceType::getUrl() const { return url; }

void TCPSourceType::setUrl(std::string urlValue) { url->setValue(std::move(urlValue)); }

void TCPSourceType::reset() {
    setUrl(url->getDefaultValue());
}

}