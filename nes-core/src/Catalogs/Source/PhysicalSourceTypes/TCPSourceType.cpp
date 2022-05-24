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

#include <Catalogs/Source/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstring>

namespace NES {

TCPSourceTypePtr TCPSourceType::create(Yaml::Node yamlConfig) {
    return std::make_shared<TCPSourceType>(TCPSourceType(std::move(yamlConfig)));
}

TCPSourceTypePtr TCPSourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<TCPSourceType>(TCPSourceType(std::move(sourceConfigMap)));
}

TCPSourceTypePtr TCPSourceType::create() { return std::make_shared<TCPSourceType>(TCPSourceType()); }

TCPSourceType::TCPSourceType()
    : PhysicalSourceType(TCP_SOURCE),
      socketHost(Configurations::ConfigurationOption<std::string>::create(Configurations::SOCKET_HOST_CONFIG,
                                                                          "127.0.0.1",
                                                                          "host to connect to")),
      socketPort(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::SOCKET_PORT_CONFIG, 3000, "port to connect to")),
      socketDomain(Configurations::ConfigurationOption<uint32_t>::create(
          Configurations::SOCKET_DOMAIN_CONFIG,
          AF_INET,
          "Domain argument specifies a communication domain; this selects the protocol family which will be used for "
          "communication. Common choices: AF_INET (IPv4 Internet protocols), AF_INET6 (IPv6 Internet protocols)")),
      socketType(Configurations::ConfigurationOption<uint32_t>::create(
          Configurations::SOCKET_TYPE_CONFIG,
          SOCK_STREAM,
          "The  socket  has  the indicated type, which specifies the communication semantics. SOCK_STREAM Provides sequenced, "
          "reliable, two-way, connection-based byte  streams.  An out-of-band data transmission mechanism may be supported, "
          "SOCK_DGRAM Supports datagrams (connectionless, unreliable messages of a fixed maximum length), "
          "SOCK_SEQPACKET Provides  a  sequenced,  reliable,  two-way connection-based data transmission path for "
          "datagrams  of  fixed maximum  length;  a consumer is required to read an entire packet with each input system call, "
          "SOCK_RAW Provides raw network protocol access, "
          "SOCK_RDM Provides a reliable datagram layer that does not  guarantee ordering")),
      socketBufferSize(Configurations::ConfigurationOption<uint32_t>::create(
          Configurations::SOCKET_BUFFER_SIZE_CONFIG,
          0,
          "Defines the socket size for one message. If 0 NES assumes that the size of the message is send in an extra message "
          "before each message. The extra message is assumed to be 4 bytes long.")),
      flushIntervalMS(Configurations::ConfigurationOption<float>::create("flushIntervalMS",
                                                                         -1,
                                                                         "tupleBuffer flush interval in milliseconds")),
      inputFormat(Configurations::ConfigurationOption<Configurations::InputFormat>::create(
          Configurations::INPUT_FORMAT_CONFIG,
          Configurations::CSV,
          "Source type defines how the data will arrive in NES. Current Option: CSV (comma separated list)")) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

TCPSourceType::TCPSourceType(std::map<std::string, std::string> sourceConfigMap) : TCPSourceType() {
    NES_INFO("TCPSourceType: Init default TCP source config object with values from command line args.");

    if (sourceConfigMap.find(Configurations::SOCKET_HOST_CONFIG) != sourceConfigMap.end()) {
        socketHost->setValue(sourceConfigMap.find(Configurations::SOCKET_HOST_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("TCPSourceType:: no socket host defined! Please define a host.");
    }
    if (sourceConfigMap.find(Configurations::SOCKET_PORT_CONFIG) != sourceConfigMap.end()) {
        socketPort->setValue(std::stoi(sourceConfigMap.find(Configurations::SOCKET_PORT_CONFIG)->second));
    } else {
        NES_THROW_RUNTIME_ERROR("TCPSourceType:: no socket port defined! Please define a port.");
    }
    if (sourceConfigMap.find(Configurations::SOCKET_DOMAIN_CONFIG) != sourceConfigMap.end()) {
        setSocketDomainViaString(sourceConfigMap.find(Configurations::SOCKET_DOMAIN_CONFIG)->second);
    }
    if (sourceConfigMap.find(Configurations::SOCKET_TYPE_CONFIG) != sourceConfigMap.end()) {
        setSocketTypeViaString(sourceConfigMap.find(Configurations::SOCKET_TYPE_CONFIG)->second);
    }
}

TCPSourceType::TCPSourceType(Yaml::Node yamlConfig) : TCPSourceType() {
    NES_INFO("TCPSourceType: Init default TCP source config object with values from YAML file.");

    if (!yamlConfig[Configurations::SOCKET_HOST_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::SOCKET_HOST_CONFIG].As<std::string>() != "\n") {
        socketHost->setValue(yamlConfig[Configurations::SOCKET_HOST_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR("TCPSourceType:: no socket host defined! Please define a host.");
    }
    if (!yamlConfig[Configurations::SOCKET_PORT_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::SOCKET_PORT_CONFIG].As<std::string>() != "\n") {
        socketPort->setValue(yamlConfig[Configurations::SOCKET_PORT_CONFIG].As<uint32_t>());
    } else {
        NES_THROW_RUNTIME_ERROR("TCPSourceType:: no socket host defined! Please define a host.");
    }
    if (!yamlConfig[Configurations::SOCKET_DOMAIN_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::SOCKET_DOMAIN_CONFIG].As<std::string>() != "\n") {
        setSocketDomainViaString(yamlConfig[Configurations::SOCKET_DOMAIN_CONFIG].As<std::string>());
    }
    if (!yamlConfig[Configurations::SOCKET_TYPE_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::SOCKET_TYPE_CONFIG].As<std::string>() != "\n") {
        setSocketTypeViaString(yamlConfig[Configurations::SOCKET_TYPE_CONFIG].As<std::string>());
    }
}

TCPSourceType::TCPSourceType()
    : PhysicalSourceType(TCP_SOURCE),
      socketHost(Configurations::ConfigurationOption<std::string>::create(Configurations::SOCKET_HOST_CONFIG,
                                                                          "127.0.0.1",
                                                                          "host to connect to")),
      socketPort(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::SOCKET_PORT_CONFIG, 3000, "port to connect to")),
      socketDomain(Configurations::ConfigurationOption<uint32_t>::create(
          Configurations::SOCKET_DOMAIN_CONFIG,
          AF_INET,
          "Domain argument specifies a communication domain; this selects the protocol family which will be used for "
          "communication. Common choices: AF_INET (IPv4 Internet protocols), AF_INET6 (IPv6 Internet protocols)")),
      socketType(Configurations::ConfigurationOption<uint32_t>::create(
          Configurations::SOCKET_TYPE_CONFIG,
          SOCK_STREAM,
          "The  socket  has  the indicated type, which specifies the communication semantics. SOCK_STREAM Provides sequenced, "
          "reliable, two-way, connection-based byte  streams.  An out-of-band data transmission mechanism may be supported, "
          "SOCK_DGRAM Supports datagrams (connectionless, unreliable messages of a fixed maximum length), "
          "SOCK_SEQPACKET Provides  a  sequenced,  reliable,  two-way connection-based data transmission path for "
          "datagrams  of  fixed maximum  length;  a consumer is required to read an entire packet with each input system call, "
          "SOCK_RAW Provides raw network protocol access, "
          "SOCK_RDM Provides a reliable datagram layer that does not  guarantee ordering")) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

std::string TCPSourceType::toString() {
    std::stringstream ss;
    ss << "TCPSourceType => {\n";
    ss << socketHost->toStringNameCurrentValue();
    ss << socketPort->toStringNameCurrentValue();
    ss << socketDomain->toStringNameCurrentValue();
    ss << socketType->toStringNameCurrentValue();
    ss << "\n}";
    return ss.str();
}

bool TCPSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<TCPSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<TCPSourceType>();
    return socketHost->getValue() == otherSourceConfig->socketHost->getValue()
        && socketPort->getValue() == otherSourceConfig->socketPort->getValue()
        && socketDomain->getValue() == otherSourceConfig->socketDomain->getValue()
        && socketType->getValue() == otherSourceConfig->socketType->getValue();
}

void TCPSourceType::reset() {
    setSocketHost(socketHost->getDefaultValue());
    setSocketPort(socketPort->getDefaultValue());
    setSocketDomain(AF_INET);
    setSocketType(SOCK_STREAM);
}

Configurations::StringConfigOption TCPSourceType::getSocketHost() const { return socketHost; }

void TCPSourceType::setSocketHost(std::string hostValue) { socketHost->setValue(std::move(hostValue)); }

Configurations::IntConfigOption TCPSourceType::getSocketPort() const { return socketPort; }

void TCPSourceType::setSocketPort(uint32_t portValue) { socketPort->setValue(portValue); }

Configurations::IntConfigOption TCPSourceType::getSocketDomain() const { return socketDomain; }

void TCPSourceType::setSocketDomain(uint32_t domainValue) { socketDomain->setValue(domainValue); }

void TCPSourceType::setSocketDomainViaString(std::string domainValue) {
    if (strcasecmp(domainValue.c_str(), "AF_INET") == 0) {
        setSocketDomain(AF_INET);
    } else if (strcasecmp(domainValue.c_str(), "AF_INET6") == 0) {
        setSocketDomain(AF_INET6);
    }
}

Configurations::IntConfigOption TCPSourceType::getSocketType() const { return socketType; }

void TCPSourceType::setSocketType(uint32_t typeValue) { socketType->setValue(typeValue); }

void TCPSourceType::setSocketTypeViaString(std::string typeValue) {
    if (strcasecmp(typeValue.c_str(), "SOCK_STREAM") == 0) {
        setSocketDomain(SOCK_STREAM);
    } else if (strcasecmp(typeValue.c_str(), "SOCK_DGRAM") == 0) {
        setSocketDomain(SOCK_DGRAM);
    } else if (strcasecmp(typeValue.c_str(), "SOCK_SEQPACKET") == 0) {
        setSocketDomain(SOCK_SEQPACKET);
    } else if (strcasecmp(typeValue.c_str(), "SOCK_RAW") == 0) {
        setSocketDomain(SOCK_RAW);
    } else if (strcasecmp(typeValue.c_str(), "SOCK_RDM") == 0) {
        setSocketDomain(SOCK_RDM);
    }
}

}// namespace NES