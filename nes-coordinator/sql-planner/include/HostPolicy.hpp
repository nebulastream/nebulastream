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

#include <algorithm>
#include <cctype>
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include <Identifiers/Identifiers.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// A statement may omit its HOST clause, and the policy decides what that means.
/// RequireHostConfig rejects such a statement.
/// DefaultHost substitutes a fixed address, which embedded deployments use because the only worker is the local one.
struct RequireHostConfig
{
};

struct DefaultHost
{
    std::string hostName;
};

using HostPolicy = std::variant<RequireHostConfig, DefaultHost>;

/// Returns whether an address ends in a port, which every worker address has.
/// The coordinator's address parser is the authority on the rest of the grammar, so this only rejects what it could never accept,
/// and leaves an address that has a port to be read there.
inline bool namesAPort(const std::string_view address)
{
    const auto colon = address.rfind(':');
    return colon != std::string_view::npos and colon + 1 < address.size()
        and std::ranges::all_of(address.substr(colon + 1), [](const unsigned char digit) { return std::isdigit(digit) != 0; });
}

/// Returns the address a statement is placed on: the one it named, or the one the policy supplies.
/// The subject is the config group of the missing HOST, so a rejection names the clause that has to supply it.
inline std::expected<std::string, Exception>
resolveHost(const std::optional<Host>& host, const HostPolicy& hostPolicy, const std::string_view subject)
{
    if (host.has_value())
    {
        /// A worker is addressed by host and port, so a value without a port names no worker and nothing can be placed on it.
        /// Reporting that here names the clause and the value, rather than leaving the address parser to fail without either.
        if (not namesAPort(host->getRawValue()))
        {
            return std::unexpected{PlacementFailure(
                "`{}`.`HOST` is '{}', which names no worker: an address is a host and a port", subject, host->getRawValue())};
        }
        return host->getRawValue();
    }
    return std::visit(
        Overloaded{
            [](const DefaultHost& defaultHost) -> std::expected<std::string, Exception> { return defaultHost.hostName; },
            [subject](const RequireHostConfig&) -> std::expected<std::string, Exception>
            { return std::unexpected{InvalidStatement("`{}`.`HOST` was not set", subject)}; }},
        hostPolicy);
}

}
