// /*
//     Licensed under the Apache License, Version 2.0 (the "License");
//     you may not use this file except in compliance with the License.
//     You may obtain a copy of the License at
//         https://www.apache.org/licenses/LICENSE-2.0
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.
// *

#include <Catalogs/Source/PhysicalSourceTypes/LoRaWANProxySourceType.hpp>

namespace NES {

LoRaWANProxySourceTypePtr LoRaWANProxySourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<LoRaWANProxySourceType>(LoRaWANProxySourceType(std::move(sourceConfigMap)));
}

LoRaWANProxySourceTypePtr LoRaWANProxySourceType::create(const Yaml::Node& yamlConfig) {
    return std::make_shared<LoRaWANProxySourceType>(LoRaWANProxySourceType(yamlConfig));
}

LoRaWANProxySourceTypePtr LoRaWANProxySourceType::create() { return std::make_shared<LoRaWANProxySourceType>(LoRaWANProxySourceType()); }

LoRaWANProxySourceType::LoRaWANProxySourceType()
    : PhysicalSourceType(LORAWAN_SOURCE),
      networkStack(Configurations::ConfigurationOption<std::string>::create(Configurations::LORAWAN_NETWORK_STACK_CONFIG,
                                                                            "ChirpStack",
                                                                            "Name of network stack")),
      url(Configurations::ConfigurationOption<std::string>::create(Configurations::URL_CONFIG,
                                                                   "127.0.0.1",
                                                                   "Address of network stack to connect to")),
      userName(Configurations::ConfigurationOption<std::string>::create(Configurations::USER_NAME_CONFIG,
                                                                        "",
                                                                        "userName for MQTT broker for the chosen network stack")),
      password(Configurations::ConfigurationOption<std::string>::create(Configurations::PASSWORD_CONFIG,
                                                                        "",
                                                                        "Password for the MQTT broker for the chosen network stack")),
      appId(Configurations::ConfigurationOption<std::string>::create(Configurations::LORAWAN_APP_ID_CONFIG,
                                                                     "",
                                                                     "AppId for the configured application in the chosen network stack"))
{
    NES_INFO(Configurations::LORAWAN_PROXY_SOURCE_CONFIG + "Init source config object with default values");
}

LoRaWANProxySourceType::LoRaWANProxySourceType(std::map<std::string, std::string> sourceConfigMap)
    : LoRaWANProxySourceType() {
        NES_INFO("LoRaWANProxySourceType: Init default LoRaWANSource config object with values from command line args");

        if (sourceConfigMap.contains(Configurations::LORAWAN_NETWORK_STACK_CONFIG)){
                networkStack->setValue(sourceConfigMap.find(Configurations::LORAWAN_NETWORK_STACK_CONFIG)->second);
        }
        if (sourceConfigMap.contains(Configurations::URL_CONFIG)){
            networkStack->setValue(sourceConfigMap.find(Configurations::URL_CONFIG)->second);
        }
        if (sourceConfigMap.contains(Configurations::USER_NAME_CONFIG)){
            networkStack->setValue(sourceConfigMap.find(Configurations::USER_NAME_CONFIG)->second);

        } else {
            NES_THROW_RUNTIME_ERROR("LoRaWANProxySourceType: no username defined! Please define a username.");
        }
        if (sourceConfigMap.contains(Configurations::PASSWORD_CONFIG)){
            networkStack->setValue(sourceConfigMap.find(Configurations::PASSWORD_CONFIG)->second);

        } else {
            NES_THROW_RUNTIME_ERROR("LoRaWANProxySourceType: no password defined! Please define a password.");
        }
        if (sourceConfigMap.contains(Configurations::LORAWAN_APP_ID_CONFIG)){
            networkStack->setValue(sourceConfigMap.find(Configurations::LORAWAN_APP_ID_CONFIG)->second);

        } else {
            NES_THROW_RUNTIME_ERROR("LoRaWANProxySourceType: no appId defined! Please define an appId.");
        }
}



LoRaWANProxySourceType::LoRaWANProxySourceType(Yaml::Node yamlConfig)
    : LoRaWANProxySourceType() {
    NES_INFO("LoRaWANProxySourceType: Init default LoRaWANSource config object with values from command line args");

    auto hasValue = [&yamlConfig](const std::string& name) {
        auto value = yamlConfig[name];
        return !value.As<std::string>().empty() && value.As<std::string>() != "\n";
    };

    if (hasValue(Configurations::LORAWAN_NETWORK_STACK_CONFIG)){
        networkStack->setValue(yamlConfig[Configurations::LORAWAN_NETWORK_STACK_CONFIG].As<std::string>());
    }
    if (hasValue(Configurations::URL_CONFIG)){
        url->setValue(yamlConfig[Configurations::URL_CONFIG].As<std::string>());
    }
    if (hasValue(Configurations::USER_NAME_CONFIG)){
        userName->setValue(yamlConfig[Configurations::USER_NAME_CONFIG].As<std::string>());

    } else {
        NES_THROW_RUNTIME_ERROR("LoRaWANProxySourceType: no username defined! Please define a username.");
    }
    if (hasValue(Configurations::PASSWORD_CONFIG)){
        password->setValue(yamlConfig[Configurations::PASSWORD_CONFIG].As<std::string>());

    } else {
        NES_THROW_RUNTIME_ERROR("LoRaWANProxySourceType: no password defined! Please define a password.");
    }
    if (hasValue(Configurations::LORAWAN_APP_ID_CONFIG)){
        appId->setValue(yamlConfig[Configurations::LORAWAN_APP_ID_CONFIG].As<std::string>());

    } else {
        NES_THROW_RUNTIME_ERROR("LoRaWANProxySourceType: no appId defined! Please define an appId.");
    }
}


Configurations::StringConfigOption LoRaWANProxySourceType::getNetworkStack() {
    return networkStack;
}

Configurations::StringConfigOption LoRaWANProxySourceType::getUrl() {
    return url;
}

Configurations::StringConfigOption LoRaWANProxySourceType::getUserName() {
    return userName;
}

Configurations::StringConfigOption LoRaWANProxySourceType::getPassword() {
    return password;
}

Configurations::StringConfigOption LoRaWANProxySourceType::getAppId() {
    return appId;
}

void LoRaWANProxySourceType::setNetworkStack(std::string networkStackValue) {
    networkStack->setValue(std::move(networkStackValue));
}

void LoRaWANProxySourceType::setUrl(std::string urlValue) {
    url->setValue(std::move(urlValue));
}

void LoRaWANProxySourceType::setUserName(std::string userNameValue) {
    userName->setValue(std::move(userNameValue));
}

void LoRaWANProxySourceType::setPassword(std::string passwordValue) {
    password->setValue(std::move(passwordValue));
}

void LoRaWANProxySourceType::setAppId(std::string appIdValue) {
    appId->setValue(std::move(appIdValue));
}

void LoRaWANProxySourceType::reset() {
    setNetworkStack(networkStack->getDefaultValue());
    setUrl(url->getDefaultValue());
    setUserName(userName->getDefaultValue());
    setPassword(password->getDefaultValue());
    setAppId(appId->getDefaultValue());
}
std::string LoRaWANProxySourceType::toString() {
    std::stringstream ss;
    ss << "LoRaWANProxySourceType =>  {\n";
    ss << Configurations::LORAWAN_NETWORK_STACK_CONFIG + ":" + networkStack->toStringNameCurrentValue();
    ss << Configurations::URL_CONFIG + ":" + url->toStringNameCurrentValue();
    ss << Configurations::USER_NAME_CONFIG + ":" + userName->toStringNameCurrentValue();
    //might be bad style to expose this
    ss << Configurations::PASSWORD_CONFIG + ":" + password->toStringNameCurrentValue();
    ss << Configurations::LORAWAN_APP_ID_CONFIG + ":" + appId->toStringNameCurrentValue();
    ss << "\n}";
    return ss.str();
}
bool LoRaWANProxySourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<LoRaWANProxySourceType>()) {
        return false;
    }
    auto otherConfig = other->as<LoRaWANProxySourceType>();

    return networkStack->getValue() == otherConfig->networkStack->getValue()
        && url->getValue()          == otherConfig->url->getValue()
        && userName->getValue()     == otherConfig->userName->getValue()
        && password->getValue()     == otherConfig->password->getValue()
        && appId->getValue()        == otherConfig->appId->getValue();
}


}// namespace NES