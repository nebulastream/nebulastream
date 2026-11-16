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

#include <optional>
#include <string>
#include <string_view>

#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Returns the worker address of a source or sink.
/// Without a HOST clause, the default host is used.
/// The coordinator sets a default host only in embedded mode, where its in-process worker is the only one.
inline Host resolveHost(const std::optional<Host>& host, const std::optional<Host>& defaultHost, const std::string_view clause)
{
    if (not host.has_value())
    {
        if (not defaultHost.has_value())
        {
            throw InvalidStatement("`{}`.`HOST` was not set", clause);
        }
        return *defaultHost;
    }
    if (not host->getRawValue().contains(':'))
    {
        throw PlacementFailure("`{}`.`HOST` is '{}': an address without a port is invalid", clause, host->getRawValue());
    }
    return *host;
}

}
