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

#include <Configurations/Validation/EndpointValidation.hpp>

#include <cstddef>
#include <expected>
#include <regex>
#include <string>
#include <string_view>
#include <Util/Strings.hpp>
#include <arpa/inet.h>
#include <fmt/format.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// Maximum valid port number (2^16 - 1)
constexpr size_t MaxPortNumber = 65535;

/// Minimum valid port number
constexpr size_t MinPortNumber = 1;

/// Maximum hostname length per RFC 1035
constexpr size_t MaxHostnameLength = 253;

std::expected<void, std::string> validatePort(std::string_view portStr)
{
    if (portStr.empty())
    {
        return std::unexpected("Port number is empty");
    }

    auto portResult = from_chars<size_t>(portStr);
    if (!portResult)
    {
        return std::unexpected(fmt::format("Invalid port format: '{}' is not valid", portStr));
    }

    if (*portResult < MinPortNumber || *portResult > MaxPortNumber)
    {
        return std::unexpected(fmt::format("Port number out of valid range ({}-{}): {}", MinPortNumber, MaxPortNumber, *portResult));
    }

    return {};
}

std::expected<void, std::string> validateIPv6Address(std::string_view address)
{
    /// Extract [host]:port format
    const size_t closeBracket = address.find(']');
    if (closeBracket == std::string_view::npos)
    {
        return std::unexpected("Invalid IPv6 format: missing closing bracket");
    }

    if (closeBracket + 1 >= address.length())
    {
        return std::unexpected("No port specified in IPv6 endpoint address");
    }

    if (address[closeBracket + 1] != ':')
    {
        return std::unexpected("Invalid IPv6 format: expected ':' after ']'");
    }

    std::string_view host = address.substr(1, closeBracket - 1);
    const std::string_view portStr = address.substr(closeBracket + 2);

    /// Validate port
    auto portResult = validatePort(portStr);
    if (!portResult)
    {
        return std::unexpected(portResult.error());
    }

    /// Validate IPv6
    const std::string hostStr(host);
    in6_addr addr6{};
    if (inet_pton(AF_INET6, hostStr.c_str(), &addr6) != 1)
    {
        return std::unexpected(fmt::format("Invalid IPv6 address: '{}'", host));
    }

    return {};
}

bool isValidIPv4(std::string_view host)
{
    const std::string hostStr(host);
    in_addr addr4{};
    return inet_pton(AF_INET, hostStr.c_str(), &addr4) == 1;
}

bool isValidHostname(std::string_view host)
{
    if (host.length() > MaxHostnameLength)
    {
        return false;
    }

    const std::regex hostnamePattern("^[a-zA-Z0-9]([a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])?"
                                     "(\\.[a-zA-Z0-9]([a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])?)*$");

    return std::regex_match(host.begin(), host.end(), hostnamePattern);
}

std::expected<void, std::string> validateBindAddress(std::string_view address, bool allowEmptyHost)
{
    if (address.empty())
    {
        return std::unexpected("Endpoint address cannot be empty");
    }

    /// Check for IPv6 format: [host]:port
    if (address[0] == '[')
    {
        return validateIPv6Address(address);
    }

    /// Find the last colon for port separation
    const size_t colonPos = address.rfind(':');
    if (colonPos == std::string_view::npos)
    {
        return std::unexpected("No port specified in endpoint address");
    }

    std::string_view host = address.substr(0, colonPos);
    const std::string_view portStr = address.substr(colonPos + 1);

    /// Validate port
    auto portResult = validatePort(portStr);
    if (!portResult)
    {
        return std::unexpected(portResult.error());
    }

    /// Empty host means bind to all interfaces (e.g., ":8080")
    if (host.empty())
    {
        if (!allowEmptyHost)
        {
            /// GRPC does not allow binding to to an empty host
            return std::unexpected("host cannot be empty");
        }
        return {};
    }

    /// Try IPv4 validation
    if (isValidIPv4(host))
    {
        return {};
    }

    /// Try hostname validation
    if (!isValidHostname(host))
    {
        return std::unexpected(fmt::format("Invalid hostname: '{}'", host));
    }

    return {};
}


}

bool EndpointValidation::isValid(const std::string& endpointString) const
{
    if (auto result = validateBindAddress(endpointString, type != GRPC); !result)
    {
        throw InvalidConfigParameter(result.error());
    }
    return true;
}
}
