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

#ifndef NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_TCPSOURCETYPE_HPP
#define NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_TCPSOURCETYPE_HPP

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/yaml/Yaml.hpp>
#include <sys/socket.h>

namespace NES {

class TCPSourceType;
using TCPSourceTypePtr = std::shared_ptr<TCPSourceType>;

class TCPSourceType : public PhysicalSourceType {

  public:
    /**
     * @brief create a TCPSourceTypePtr object
     * @param sourceConfigMap inputted config options
     * @return TCPSourceTypePtr
     */
    static TCPSourceTypePtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a TCPSourceTypePtr object
     * @param yamlConfig inputted config options
     * @return TCPSourceTypePtr
     */
    static TCPSourceTypePtr create(Yaml::Node yamlConfig);

    /**
     * @brief create a TCPSourceTypePtr object with default values
     * @return TCPSourceTypePtr
     */
    static TCPSourceTypePtr create();

    /**
     * @brief converts configs to string
     * @return string of configs
     */
    std::string toString() override;

    /**
     * @brief checks if two config objects are equal
     * @param other other config object
     * @return true if equal, false otherwise
     */
    bool equal(const PhysicalSourceTypePtr& other) override;

    /**
     * @brief set config obtions to default
     */
    void reset() override;

    /**
     * @brief set host
     * @param host new socket host
     */
    void setSocketHost(std::string host);

    /**
     * @brief get host address
     * @return host adress
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getSocketHost() const;

    /**
     * @brief set port
     * @param port new socket port
     */
    void setSocketPort(uint32_t port);

    /**
     * @brief get port
     * @return port
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getSocketPort() const;

    /**
     * @brief get the domain
     * @return domain
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getSocketDomain() const;

    /**
     * @brief set the domain
     * @param domain string with domain to be set
     */
    void setSocketDomain(uint32_t domain);

    /**
     * @brief set the domain via string
     * @param domain string viable options: AF_INET IPv4 Internet protocols, AF_INET6 IPv6 Internet protocols
     */
    void setSocketDomainViaString(std::string domain);

    /**
     * @brief get the socket type
     * @return socket type
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getSocketType() const;

    /**
     * @brief set the socket type
     * @param type the type to be set
     */
    void setSocketType(uint32_t type);

    /**
     * @brief set the type via string
     * @param type string viable options:
     * SOCK_STREAM Provides sequenced, reliable, two-way, connection-based byte  streams.  An out-of-band data transmission mechanism may be supported,
     * SOCK_DGRAM Supports datagrams (connectionless, unreliable messages of a fixed maximum length),
     * SOCK_SEQPACKET Provides  a  sequenced,  reliable,  two-way connection-based data transmission path  for  datagrams  of  fixed maximum  length;  a consumer is required to read an entire packet with each input system call,
     * SOCK_RAW Provides raw network protocol access,
     * SOCK_RDM Provides a reliable datagram layer that does not  guarantee ordering
     */
    void setSocketTypeViaString(std::string type);

    /**
     * @brief Sets the input data format given as Configuration::InputFormat
     * @param inputFormatValue
     */
    void setInputFormat(Configurations::InputFormat inputFormatValue);

    /**
     * @brief Get the input data format given as Configuration::InputFormat
     * @return inputFormatValue
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<Configurations::InputFormat>> getInputFormat() const;

    /**
     * @brief Get tupleBuffer flush interval in milliseconds
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<float>> getFlushIntervalMS() const;

    /**
     * @brief Set tupleBuffer flush interval in milliseconds
     */
    void setFlushIntervalMS(float flushIntervalMs);

    /**
     * @brief get message divided for TCPSource
     * @return message divider
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<char>> getTupleSeparator() const;

    /**
     * @brief set message divider for TCPSource
     * @param deviderTokenValue message divider as char
     */
    void setTupleSeparator(char dividerTokenValue);

  private:
    /**
     * @brief constructor to create a new TCP source type object initialized with values from sourceConfigMap
     * @param sourceConfigMap inputted config options
     */
    explicit TCPSourceType(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new TCP source type object initialized with values from yamlConfig
     * @param yamlConfig inputted config options
     */
    explicit TCPSourceType(Yaml::Node yamlConfig);

    /**
     * @brief constructor to create a new TCP source type object initialized with default values
     */
    TCPSourceType();

    Configurations::StringConfigOption socketHost;
    Configurations::IntConfigOption socketPort;
    Configurations::IntConfigOption socketDomain;
    Configurations::IntConfigOption socketType;
    Configurations::FloatConfigOption flushIntervalMS;
    Configurations::InputFormatConfigOption inputFormat;
    Configurations::CharConfigOption tupleSeparator;

};
}// namespace NES
#endif//NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_TCPSOURCETYPE_HPP
