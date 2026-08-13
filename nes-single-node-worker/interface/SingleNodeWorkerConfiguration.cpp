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

#include <SingleNodeWorkerConfiguration.hpp>

#include <expected>
#include <string>
#include <utility>
#include <Configuration/WorkerConfiguration.hpp>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Configurations/Validation/EndpointValidation.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Variant.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

/// NOLINTBEGIN(cert-err58-cpp)
const ConfigField<std::string> DATA_ADDRESS{
    Identifier::parse("data_address"), "Data-plane address. This is the {Hostname}:{PORT}", std::string{}};

const ConfigField<std::string> GRPC_ADDRESS_URI{
    Identifier::parse("grpc"),
    R"(The address to try to bind to the server in URI form. If the scheme name is omitted, "dns:///" is assumed. To bind to any address, please use IPv6 any, i.e., [::]:<port>, which also accepts IPv4 connections. Valid values include dns:///localhost:1234, 192.168.1.1:31416, dns:///[::1]:27182, etc.)",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<std::string>(literal, expectedType<std::string>())
            .and_then(
                [](std::string&& value) -> std::expected<std::string, Exception>
                {
                    if (!EndpointValidation{EndpointValidation::GRPC}.isValid(value))
                    {
                        return std::unexpected{InvalidConfigParameter("Invalid gRPC endpoint: {}", value)};
                    }
                    return std::move(value);
                });
    },
    std::string{"[::]:8080"}};

const ConfigField<bool> ENABLE_GOOGLE_EVENT_TRACE{
    Identifier::parse("enable_event_trace"),
    "Enable Google Event Trace logging that generates Chrome tracing compatible JSON files for performance analysis.",
    false};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> SingleNodeWorkerConfiguration::getConfigSchema()
{
    return createConfigSchema(DATA_ADDRESS, GRPC_ADDRESS_URI, ENABLE_GOOGLE_EVENT_TRACE, WorkerConfiguration::getConfigSchema());
}

SingleNodeWorkerConfiguration SingleNodeWorkerConfiguration::fromConfig(const InstantiatedConfig& config)
{
    return {
        .dataAddress = config.get(DATA_ADDRESS),
        .grpcAddressUri = config.get(GRPC_ADDRESS_URI),
        .enableGoogleEventTrace = config.get(ENABLE_GOOGLE_EVENT_TRACE),
        .workerConfiguration = WorkerConfiguration::fromConfig(config)};
}
}
