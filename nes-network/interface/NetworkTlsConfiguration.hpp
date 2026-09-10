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

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/NonZeroValidation.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <ErrorHandling.hpp>
#include <NetworkOptions.hpp>

namespace NES
{

/// TLS applies to every worker network connection, including control and data channels.
class NetworkTlsConfiguration final : public BaseConfiguration
{
public:
    NetworkTlsConfiguration() = default;
    NetworkTlsConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    BoolOption enabled = {"enabled", "false", "Require mutual TLS 1.3 for worker network connections."};
    StringOption certificateFile
        = {"certificate_file", "", "PEM certificate chain used for server and client authentication (leaf first)."};
    StringOption privateKeyFile = {"private_key_file", "", "PEM private key for this worker's certificate."};
    StringOption caFile = {"ca_file", "", "PEM CA bundle used to verify peer certificates in both directions."};
    UIntOption handshakeTimeoutMs
        = {"handshake_timeout_ms",
           "10000",
           "Timeout in milliseconds for TLS establishment.",
           {std::make_shared<NumberValidation>(), std::make_shared<NonZeroValidation>()}};

    NetworkTlsOptions toOptions() const
    {
        if (!enabled.getValue())
        {
            if (!certificateFile.getValue().empty() || !privateKeyFile.getValue().empty() || !caFile.getValue().empty())
            {
                throw InvalidConfigParameter("TLS certificate settings require worker.network.tls.enabled=true");
            }
            return NoTLS{};
        }

        const auto timeoutMs = handshakeTimeoutMs.getValue();
        if (timeoutMs == 0 || !std::in_range<std::chrono::milliseconds::rep>(timeoutMs))
        {
            throw InvalidConfigParameter("worker.network.tls.handshake_timeout_ms must be positive and fit in std::chrono::milliseconds");
        }
        return TLS{
            .certificateFile = std::filesystem::path{certificateFile.getValue()},
            .privateKeyFile = std::filesystem::path{privateKeyFile.getValue()},
            .caFile = std::filesystem::path{caFile.getValue()},
            .handshakeTimeout = std::chrono::milliseconds{static_cast<std::chrono::milliseconds::rep>(timeoutMs)},
        };
    }

private:
    std::vector<BaseOption*> getOptions() override { return {&enabled, &certificateFile, &privateKeyFile, &caFile, &handshakeTimeoutMs}; }
};
}
