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

#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstring>
#include <utility>

namespace NES {

TCPSourceTypePtr TCPSourceType::create(const std::string& logicalSourceName, const std::string& physicalSourceName, Yaml::Node yamlConfig) {
    return std::make_shared<TCPSourceType>(
        TCPSourceType(logicalSourceName, physicalSourceName, std::move(yamlConfig)));
}

TCPSourceTypePtr TCPSourceType::create(const std::string& logicalSourceName,
                                       const std::string& physicalSourceName,
                                       std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<TCPSourceType>(TCPSourceType(logicalSourceName, physicalSourceName, std::move(sourceConfigMap)));
}

TCPSourceTypePtr TCPSourceType::create(const std::string& logicalSourceName, const std::string& physicalSourceName) {
    return std::make_shared<TCPSourceType>(TCPSourceType(logicalSourceName, physicalSourceName));
}

TCPSourceType::TCPSourceType(const std::string& logicalSourceName, const std::string& physicalSourceName)
    : PhysicalSourceType(logicalSourceName, physicalSourceName, SourceType::TCP_SOURCE),
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
      flushIntervalMS(Configurations::ConfigurationOption<float>::create("flushIntervalMS",
                                                                         -1,
                                                                         "tupleBuffer flush interval in milliseconds")),
      inputFormat(Configurations::ConfigurationOption<Configurations::InputFormat>::create(
          Configurations::INPUT_FORMAT_CONFIG,
          Configurations::InputFormat::CSV,
          "Input format defines how the data will arrive in NES. Current Option: CSV (comma separated list with separator "
          "between lines/tuples), JSON.")),
      decideMessageSize(Configurations::ConfigurationOption<Configurations::TCPDecideMessageSize>::create(
          Configurations::DECIDE_MESSAGE_SIZE_CONFIG,
          Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR,
          "Decide how a message size is obtained: TUPLE_SEPARATOR: TCP messages are send with a char acting as tuple separator "
          "between them, tupleSeperator needs to be set USER_SPECIFIED_BUFFER_SIZE: User specifies the buffer size beforehand, "
          "socketBufferSize needs to be set BUFFER_SIZE_FROM_SOCKET: Between each message you also obtain a fixed amount of "
          "bytes with the size of the next message, bytesUsedForSocketBufferSizeTransfer needs to be set.")),
      tupleSeparator(Configurations::ConfigurationOption<char>::create(
          Configurations::TUPLE_SEPARATOR_CONFIG,
          '\n',
          "Tuple separator defines how the incoming TCP messages can be distinguished into tuples.")),
      socketBufferSize(Configurations::ConfigurationOption<uint32_t>::create(Configurations::SOCKET_BUFFER_SIZE_CONFIG,
                                                                             0,
                                                                             "Size of a message send via TCP connection")),
      bytesUsedForSocketBufferSizeTransfer(Configurations::ConfigurationOption<uint32_t>::create(
          Configurations::BYTES_USED_FOR_SOCKET_BUFFER_SIZE_TRANSFER_CONFIG,
          0,
          "Number of bytes used to identify the size of the next incoming message")) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

TCPSourceType::TCPSourceType(const std::string& logicalSourceName,
                             const std::string& physicalSourceName,
                             std::map<std::string, std::string> sourceConfigMap)
    : TCPSourceType(logicalSourceName, physicalSourceName) {
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
    if (sourceConfigMap.find(Configurations::FLUSH_INTERVAL_MS_CONFIG) != sourceConfigMap.end()) {
        flushIntervalMS->setValue(std::stof(sourceConfigMap.find(Configurations::FLUSH_INTERVAL_MS_CONFIG)->second));
    }
    if (sourceConfigMap.find(Configurations::DECIDE_MESSAGE_SIZE_CONFIG) != sourceConfigMap.end()) {
        decideMessageSize->setTCPDecideMessageSizeEnum(sourceConfigMap.find(Configurations::DECIDE_MESSAGE_SIZE_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR(
            "TCPSourceType: you have not decided how to obtain the size of a message! Please define decideMessageSize.");
    }
    if (sourceConfigMap.find(Configurations::INPUT_FORMAT_CONFIG) != sourceConfigMap.end()) {
        inputFormat->setInputFormatEnum(sourceConfigMap.find(Configurations::INPUT_FORMAT_CONFIG)->second);
    }
    switch (decideMessageSize->getValue()) {
        case Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR:
            if (sourceConfigMap.find(Configurations::TUPLE_SEPARATOR_CONFIG) != sourceConfigMap.end()) {
                tupleSeparator->setValue(sourceConfigMap.find(Configurations::TUPLE_SEPARATOR_CONFIG)->second[0]);
            } else {
                NES_THROW_RUNTIME_ERROR("TCPSourceType: You want to use a tuple separator to obtain the size of a message, "
                                        "however, you have not defined a tuple separator. Please define tupleSeparator");
            }
            break;
        case Configurations::TCPDecideMessageSize::USER_SPECIFIED_BUFFER_SIZE:
            if (sourceConfigMap.find(Configurations::SOCKET_BUFFER_SIZE_CONFIG) != sourceConfigMap.end()) {
                socketBufferSize->setValue(std::stoi(sourceConfigMap.find(Configurations::SOCKET_BUFFER_SIZE_CONFIG)->second));
            } else {
                NES_THROW_RUNTIME_ERROR("TCPSourceType: You want to use a pre-specified size as message size, "
                                        "however, you have not defined a socket buffer size. Please define socketBufferSize");
            }
            break;
        case Configurations::TCPDecideMessageSize::BUFFER_SIZE_FROM_SOCKET:
            if (sourceConfigMap.find(Configurations::BYTES_USED_FOR_SOCKET_BUFFER_SIZE_TRANSFER_CONFIG)
                != sourceConfigMap.end()) {
                bytesUsedForSocketBufferSizeTransfer->setValue(
                    std::stoi(sourceConfigMap.find(Configurations::BYTES_USED_FOR_SOCKET_BUFFER_SIZE_TRANSFER_CONFIG)->second));
            } else {
                NES_THROW_RUNTIME_ERROR("TCPSourceType: You want to use a fixed number of bytes send between messages to "
                                        "transfer the size of a message, "
                                        "however, you have not defined how many bytes are used for that. Please define "
                                        "bytesUsedForSocketBufferSizeTransfer");
            }
            break;
    }
}

TCPSourceType::TCPSourceType(const std::string& logicalSourceName, const std::string& physicalSourceName, Yaml::Node yamlConfig)
    : TCPSourceType(logicalSourceName, physicalSourceName) {
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
    if (!yamlConfig[Configurations::FLUSH_INTERVAL_MS_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::FLUSH_INTERVAL_MS_CONFIG].As<std::string>() != "\n") {
        flushIntervalMS->setValue(std::stof(yamlConfig[Configurations::FLUSH_INTERVAL_MS_CONFIG].As<std::string>()));
    }
    if (!yamlConfig[Configurations::INPUT_FORMAT_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::INPUT_FORMAT_CONFIG].As<std::string>() != "\n") {
        inputFormat->setInputFormatEnum(yamlConfig[Configurations::INPUT_FORMAT_CONFIG].As<std::string>());
    }
    if (!yamlConfig[Configurations::DECIDE_MESSAGE_SIZE_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::DECIDE_MESSAGE_SIZE_CONFIG].As<std::string>() != "\n") {
        decideMessageSize->setTCPDecideMessageSizeEnum(yamlConfig[Configurations::DECIDE_MESSAGE_SIZE_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR(
            "TCPSourceType: you have not decided how to obtain the size of a message! Please define decideMessageSize.");
    }
    switch (decideMessageSize->getValue()) {
        case Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR:
            if (!yamlConfig[Configurations::TUPLE_SEPARATOR_CONFIG].As<std::string>().empty()
                && yamlConfig[Configurations::TUPLE_SEPARATOR_CONFIG].As<std::string>() != "\n") {
                tupleSeparator->setValue(yamlConfig[Configurations::TUPLE_SEPARATOR_CONFIG].As<std::string>()[0]);
            } else {
                NES_THROW_RUNTIME_ERROR("TCPSourceType: You want to use a tuple separator to obtain the size of a message, "
                                        "however, you have not defined a tuple separator. Please define tupleSeparator");
            }
            break;
        case Configurations::TCPDecideMessageSize::USER_SPECIFIED_BUFFER_SIZE:
            if (!yamlConfig[Configurations::SOCKET_BUFFER_SIZE_CONFIG].As<std::string>().empty()
                && yamlConfig[Configurations::SOCKET_BUFFER_SIZE_CONFIG].As<std::string>() != "\n") {
                socketBufferSize->setValue(yamlConfig[Configurations::SOCKET_BUFFER_SIZE_CONFIG].As<uint32_t>());
            } else {
                NES_THROW_RUNTIME_ERROR("TCPSourceType: You want to use a pre-specified size as message size, "
                                        "however, you have not defined a socket buffer size. Please define socketBufferSize");
            }
            break;
        case Configurations::TCPDecideMessageSize::BUFFER_SIZE_FROM_SOCKET:
            if (!yamlConfig[Configurations::BYTES_USED_FOR_SOCKET_BUFFER_SIZE_TRANSFER_CONFIG].As<std::string>().empty()
                && yamlConfig[Configurations::BYTES_USED_FOR_SOCKET_BUFFER_SIZE_TRANSFER_CONFIG].As<std::string>() != "\n") {
                bytesUsedForSocketBufferSizeTransfer->setValue(
                    yamlConfig[Configurations::BYTES_USED_FOR_SOCKET_BUFFER_SIZE_TRANSFER_CONFIG].As<uint32_t>());
            } else {
                NES_THROW_RUNTIME_ERROR("TCPSourceType: You want to use a fixed number of bytes send between messages to "
                                        "transfer the size of a message, "
                                        "however, you have not defined how many bytes are used for that. Please define "
                                        "bytesUsedForSocketBufferSizeTransfer");
            }
            break;
    }
}

std::string TCPSourceType::toString() {
    std::stringstream ss;
    ss << "TCPSourceType => {\n";
    ss << socketHost->toStringNameCurrentValue();
    ss << socketPort->toStringNameCurrentValue();
    ss << socketDomain->toStringNameCurrentValue();
    ss << socketType->toStringNameCurrentValue();
    ss << flushIntervalMS->toStringNameCurrentValue();
    ss << inputFormat->toStringNameCurrentValueEnum();
    ss << decideMessageSize->toStringNameCurrentValueEnum();
    ss << tupleSeparator->toStringNameCurrentValue();
    ss << socketBufferSize->toStringNameCurrentValue();
    ss << bytesUsedForSocketBufferSizeTransfer->toStringNameCurrentValue();
    ss << "}";
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
        && socketType->getValue() == otherSourceConfig->socketType->getValue()
        && flushIntervalMS->getValue() == otherSourceConfig->flushIntervalMS->getValue()
        && inputFormat->getValue() == otherSourceConfig->inputFormat->getValue()
        && decideMessageSize->getValue() == otherSourceConfig->decideMessageSize->getValue()
        && tupleSeparator->getValue() == otherSourceConfig->tupleSeparator->getValue()
        && socketBufferSize->getValue() == otherSourceConfig->socketBufferSize->getValue()
        && bytesUsedForSocketBufferSizeTransfer->getValue()
        == otherSourceConfig->bytesUsedForSocketBufferSizeTransfer->getValue();
}

void TCPSourceType::reset() {
    setSocketHost(socketHost->getDefaultValue());
    setSocketPort(socketPort->getDefaultValue());
    setSocketDomain(AF_INET);
    setSocketType(SOCK_STREAM);
    setFlushIntervalMS(flushIntervalMS->getDefaultValue());
    setInputFormat(inputFormat->getDefaultValue());
    setDecideMessageSize(decideMessageSize->getDefaultValue());
    setTupleSeparator(tupleSeparator->getDefaultValue());
    setSocketBufferSize(socketBufferSize->getDefaultValue());
    setBytesUsedForSocketBufferSizeTransfer(bytesUsedForSocketBufferSizeTransfer->getDefaultValue());
}

Configurations::StringConfigOption TCPSourceType::getSocketHost() const { return socketHost; }

void TCPSourceType::setSocketHost(const std::string& hostValue) { socketHost->setValue(hostValue); }

Configurations::IntConfigOption TCPSourceType::getSocketPort() const { return socketPort; }

void TCPSourceType::setSocketPort(uint32_t portValue) { socketPort->setValue(portValue); }

Configurations::IntConfigOption TCPSourceType::getSocketDomain() const { return socketDomain; }

void TCPSourceType::setSocketDomain(uint32_t domainValue) { socketDomain->setValue(domainValue); }

void TCPSourceType::setSocketDomainViaString(const std::string& domainValue) {
    if (strcasecmp(domainValue.c_str(), "AF_INET") == 0) {
        setSocketDomain(AF_INET);
    } else if (strcasecmp(domainValue.c_str(), "AF_INET6") == 0) {
        setSocketDomain(AF_INET6);
    } else {
        NES_ERROR("TCPSourceType::setSocketDomainViaString: Value unknown.");
    }
}

Configurations::IntConfigOption TCPSourceType::getSocketType() const { return socketType; }

void TCPSourceType::setSocketType(uint32_t typeValue) { socketType->setValue(typeValue); }

void TCPSourceType::setSocketTypeViaString(std::string typeValue) {
    if (strcasecmp(typeValue.c_str(), "SOCK_STREAM") == 0) {
        setSocketType(SOCK_STREAM);
    } else if (strcasecmp(typeValue.c_str(), "SOCK_DGRAM") == 0) {
        setSocketType(SOCK_DGRAM);
    } else if (strcasecmp(typeValue.c_str(), "SOCK_SEQPACKET") == 0) {
        setSocketType(SOCK_SEQPACKET);
    } else if (strcasecmp(typeValue.c_str(), "SOCK_RAW") == 0) {
        setSocketType(SOCK_RAW);
    } else if (strcasecmp(typeValue.c_str(), "SOCK_RDM") == 0) {
        setSocketType(SOCK_RDM);
    } else {
        NES_ERROR("TCPSourceType::setSocketDomainViaString: Value unknown.");
    }
}

void TCPSourceType::setInputFormat(Configurations::InputFormat inputFormatValue) {
    inputFormat->setValue(std::move(inputFormatValue));
}
Configurations::InputFormatConfigOption TCPSourceType::getInputFormat() const { return inputFormat; }

Configurations::FloatConfigOption TCPSourceType::getFlushIntervalMS() const { return flushIntervalMS; }

void TCPSourceType::setFlushIntervalMS(float flushIntervalMs) { flushIntervalMS->setValue(flushIntervalMs); }

Configurations::CharConfigOption TCPSourceType::getTupleSeparator() const { return tupleSeparator; }

void TCPSourceType::setTupleSeparator(char tupleSeparatorValue) { tupleSeparator->setValue(tupleSeparatorValue); }

Configurations::IntConfigOption TCPSourceType::getSocketBufferSize() const { return socketBufferSize; }

void TCPSourceType::setDecideMessageSize(Configurations::TCPDecideMessageSize decideMessageSizeValue) {
    decideMessageSize->setValue(decideMessageSizeValue);
}

Configurations::TCPDecideMessageSizeConfigOption TCPSourceType::getDecideMessageSize() const { return decideMessageSize; }
void TCPSourceType::setSocketBufferSize(uint32_t socketBufferSizeValue) { socketBufferSize->setValue(socketBufferSizeValue); }
Configurations::IntConfigOption TCPSourceType::getBytesUsedForSocketBufferSizeTransfer() const {
    return bytesUsedForSocketBufferSizeTransfer;
}
void TCPSourceType::setBytesUsedForSocketBufferSizeTransfer(uint32_t bytesUsedForSocketBufferSizeTransferValue) {
    bytesUsedForSocketBufferSizeTransfer->setValue(bytesUsedForSocketBufferSizeTransferValue);
}

}// namespace NES