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

#pragma once

#include <cstdint>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <string>
#include <optional>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <Configurations/Descriptor.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES::Sources
{

using asio::ip::tcp;

class TCPSource final : public AsyncSource
{
public:
    static inline const std::string NAME = "TCP";

    explicit TCPSource(const SourceDescriptor& sourceDescriptor);
    TCPSource() = delete;
    ~TCPSource() override = default;

    TCPSource(const TCPSource&) = delete;
    TCPSource& operator=(const TCPSource&) = delete;

    TCPSource(TCPSource&&) = delete;
    TCPSource& operator=(TCPSource&&) = delete;

    asio::awaitable<InternalSourceResult, Executor> fillBuffer(IOBuffer& buffer) override;

    /// Open TCP connection.
    asio::awaitable<void, Executor> open() override;
    /// Close TCP connection.
    void close() override;

    static Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    const std::string socketHost;
    const std::string socketPort;

    std::optional<tcp::socket> socket;
};

/// Defines the names, (optional) default values, (optional) validation & config functions, for all TCP config parameters.
struct ConfigParametersTCP
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> HOST{
        "socketHost",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return Configurations::DescriptorConfig::tryGet(HOST, config); }};
    static inline const Configurations::DescriptorConfig::ConfigParameter<uint32_t> PORT{
        "socketPort",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            /// Mandatory (no default value)
            const auto portNumber = Configurations::DescriptorConfig::tryGet(PORT, config);
            if (portNumber.has_value())
            {
                constexpr uint32_t PORT_NUMBER_MAX = 65535;
                if (portNumber.value() <= PORT_NUMBER_MAX)
                {
                    return portNumber;
                }
                NES_ERROR("TCPSource specified port is: {}, but ports must be between 0 and {}", portNumber.value(), PORT_NUMBER_MAX);
            }
            return portNumber;
        }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(HOST, PORT);
};
}
