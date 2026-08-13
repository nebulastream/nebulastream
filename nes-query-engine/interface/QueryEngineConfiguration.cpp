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

#include <QueryEngineConfiguration.hpp>

#include <cstdint>
#include <expected>
#include <thread>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
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
const ConfigField<uint64_t> NUMBER_OF_WORKER_THREADS{
    Identifier::parse("number_of_worker_threads"),
    "Number of worker threads used within the QueryEngine",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<int64_t>(literal, expectedType<uint64_t>())
            .and_then(narrowConfigValue<int64_t, uint64_t>)
            .and_then(
                [](const uint64_t value) -> std::expected<uint64_t, Exception>
                {
                    if (value == 0)
                    {
                        return std::unexpected{InvalidConfigParameter("Number of worker threads cannot be zero")};
                    }
                    if (value > std::thread::hardware_concurrency())
                    {
                        return std::unexpected{InvalidConfigParameter(
                            "Cannot use more worker threads than available CPUs: {} vs. {} available CPUs",
                            value,
                            std::thread::hardware_concurrency())};
                    }
                    return value;
                });
    },
    uint64_t{4}};

const ConfigField<uint64_t> ADMISSION_QUEUE_SIZE{
    Identifier::parse("admission_queue_size"),
    "Size of the bounded admission queue used within the QueryEngine",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(narrowConfigValue<int64_t, uint64_t>); },
    uint64_t{1000}};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> QueryEngineConfiguration::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("query_engine"), NUMBER_OF_WORKER_THREADS, ADMISSION_QUEUE_SIZE);
}

QueryEngineConfiguration QueryEngineConfiguration::fromConfig(const InstantiatedConfig& config)
{
    return {config.get(NUMBER_OF_WORKER_THREADS), config.get(ADMISSION_QUEUE_SIZE)};
}
}
