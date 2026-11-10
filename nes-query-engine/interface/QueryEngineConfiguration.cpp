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
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Variant.hpp>
#include <CpuPinning.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

/// NOLINTBEGIN(bugprone-throwing-static-initialization)
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

const ConfigField<bool> CPU_PINNING{
    Identifier::parse("cpu_pinning"),
    "Pin every worker thread to a CPU of its own among the performance CPUs, one per physical core first, and all other threads of the "
    "worker to the misc CPUs",
    false};

ConfigField<std::vector<int>> cpuListField(std::string name, std::string description)
{
    return {
        Identifier::parse(name),
        std::move(description),
        [](const ConfigLiteral& literal)
        {
            /// A list of a single CPU, e.g. 8, arrives as a number
            const auto list
                = std::holds_alternative<int64_t>(literal) ? ConfigLiteral{std::to_string(std::get<int64_t>(literal))} : literal;
            return tryGetOr<std::string>(list, expectedType<std::string>())
                .and_then(
                    [](const std::string& value) -> std::expected<std::vector<int>, Exception>
                    {
                        if (auto cpus = CpuPinning::parseCpuList(value))
                        {
                            return *cpus;
                        }
                        return std::unexpected{InvalidConfigParameter("Invalid CPU list, expected e.g. 0-7,16: {}", value)};
                    });
        },
        std::vector<int>{},
        ""};
}

const auto PERFORMANCE_CPUS = cpuListField(
    "performance_cpus", "CPUs for the worker threads with cpu_pinning, e.g. 0-15. Empty: the P-cores of a hybrid CPU, otherwise all CPUs");
const auto MISC_CPUS = cpuListField(
    "misc_cpus",
    "CPUs for all other threads with cpu_pinning, e.g. 16-23. Empty: the E-cores of a hybrid CPU, otherwise the performance CPUs");
/// NOLINTEND(bugprone-throwing-static-initialization)

}

Schema<QualifiedErasedConfigField, Ordered> QueryEngineConfiguration::getConfigSchema()
{
    return createConfigSchema(
        Identifier::parse("query_engine"), NUMBER_OF_WORKER_THREADS, ADMISSION_QUEUE_SIZE, CPU_PINNING, PERFORMANCE_CPUS, MISC_CPUS);
}

QueryEngineConfiguration QueryEngineConfiguration::fromConfig(const InstantiatedConfig& config)
{
    return {
        config.get(NUMBER_OF_WORKER_THREADS),
        config.get(ADMISSION_QUEUE_SIZE),
        config.get(CPU_PINNING),
        config.get(PERFORMANCE_CPUS),
        config.get(MISC_CPUS)};
}
}
