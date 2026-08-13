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
#include <string>
#include <utility>
#include <Configuration/WorkerConfiguration.hpp>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

struct SingleNodeWorkerConfiguration
{
    /// Root schema of the worker CLI: top-level fields are unprefixed, the worker sub-schema
    /// nests under `worker.*` as in the old option tree.
    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    SingleNodeWorkerConfiguration() = delete;

    SingleNodeWorkerConfiguration(
        std::string dataAddress, std::string grpcAddressUri, bool enableGoogleEventTrace, WorkerConfiguration workerConfiguration)
        : dataAddress(std::move(dataAddress))
        , grpcAddressUri(std::move(grpcAddressUri))
        , enableGoogleEventTrace(enableGoogleEventTrace)
        , workerConfiguration(std::move(workerConfiguration))
    {
    }

    /// Data-plane address. This is the {Hostname}:{PORT}
    std::string dataAddress;

    /// GRPC Server Address URI. By default, it binds to any address and listens on port 8080
    std::string grpcAddressUri;

    /// Enable Google Event Trace logging (Chrome tracing format)
    bool enableGoogleEventTrace;

    WorkerConfiguration workerConfiguration;

    static SingleNodeWorkerConfiguration fromConfig(const InstantiatedConfig& config);
};
}
