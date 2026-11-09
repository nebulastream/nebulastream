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

#include <QueryExecutionConfiguration.hpp>

#include <cstdint>
#include <expected>
#include <string>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Variant.hpp>
#include <fmt/format.h>
#include <BloomFilterConfiguration.hpp>
#include <ErrorHandling.hpp>
#include <SliceCacheConfiguration.hpp>

namespace NES
{

namespace
{

/// The largest fixed header, of the paged vector (64 bytes), has to fit into a buffer and a page, with room for a few records behind it.
/// Whether a record or hash map entry of the query fits is checked when lowering it.
constexpr uint64_t MINIMUM_BUFFER_SIZE = 256;

/// NOLINTBEGIN(bugprone-throwing-static-initialization)
const ConfigField<ExecutionMode> EXECUTION_MODE{
    Identifier::parse("execution_mode"),
    fmt::format("Execution mode for the query compiler: {}", enumPipeList<ExecutionMode>()),
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<std::string>(literal, expectedType<std::string>())
            .and_then(
                [](std::string value) -> std::expected<ExecutionMode, Exception>
                {
                    if (const auto parsed = EnumWrapper{value}.asEnum<ExecutionMode>())
                    {
                        return *parsed;
                    }
                    return std::unexpected{InvalidConfigParameter("Invalid execution mode, must be COMPILER or INTERPRETER: {}", value)};
                });
    },
    ExecutionMode::COMPILER,
    "COMPILER"};

const ConfigField<bool> NAUTILUS_INLINING{
    Identifier::parse("nautilus_inlining"),
    "Enables proxy function inlining in nautilus. (only relevant with COMPILER execution mode). Defaults to whether this build "
    "applied the inlining pass (NES_ENABLE_NAUTILUS_INLINING); enabling it without the pass has no effect.",
    NES_NAUTILUS_INLINING_ACTIVE};

const ConfigField<bool> TUPLE_BUFFER_INLINING{
    Identifier::parse("tuple_buffer_inlining"),
    "Stores the first records of a paged data structure in its main buffer instead of a separate page, which saves buffers for "
    "structures holding few records (e.g., the per-key paged vectors of the hash join).",
    true};

const ConfigField<uint64_t> NUMBER_OF_PARTITIONS{
    Identifier::parse("number_of_partitions"),
    "Number of buckets in a hash table. Fixed at query-compilation time and never grown: the hash maps do not rehash, so too low "
    "a value lengthens the chains, while too high a one costs a chain pointer per hash map, worker thread and slice.",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<int64_t>(literal, expectedType<uint64_t>())
            .and_then(narrowConfigValue<int64_t, uint64_t>)
            .and_then(
                [](const uint64_t value) -> std::expected<uint64_t, Exception>
                {
                    if (value == 0)
                    {
                        return std::unexpected{InvalidConfigParameter("number_of_partitions must not be zero")};
                    }
                    return value;
                });
    },
    uint64_t{1024}};

/// Pages are pooled only when equal to OPERATOR_BUFFER_SIZE; otherwise they are unpooled
/// If pages tend to hold only a few elements (wasting space), lower both values together, keeping them equal.
/// Eventually we will optimize the tuning of these parameters at runtime or query submission based on data stream characteristics.
const ConfigField<uint64_t> PAGE_SIZE{
    Identifier::parse("page_size"),
    "Page size of any other paged data structure",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<int64_t>(literal, expectedType<uint64_t>())
            .and_then(narrowConfigValue<int64_t, uint64_t>)
            .and_then(
                [](const uint64_t value) -> std::expected<uint64_t, Exception>
                {
                    if (value < MINIMUM_BUFFER_SIZE)
                    {
                        return std::unexpected{
                            InvalidConfigParameter("page_size must be at least {} bytes, but is {}", MINIMUM_BUFFER_SIZE, value)};
                    }
                    return value;
                });
    },
    uint64_t{4096}};

const ConfigField<uint64_t> NUMBER_OF_RECORDS_PER_KEY{
    Identifier::parse("number_of_records_per_key"),
    "Expected number of records per key, for example in a hash join. If set too low or high affects the performance.",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(narrowConfigValue<int64_t, uint64_t>); },
    uint64_t{10}};

const ConfigField<uint64_t> OPERATOR_BUFFER_SIZE{
    Identifier::parse("operator_buffer_size"),
    "Buffer size of a operator e.g. during scan",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<int64_t>(literal, expectedType<uint64_t>())
            .and_then(narrowConfigValue<int64_t, uint64_t>)
            .and_then(
                [](const uint64_t value) -> std::expected<uint64_t, Exception>
                {
                    if (value < MINIMUM_BUFFER_SIZE)
                    {
                        return std::unexpected{InvalidConfigParameter(
                            "operator_buffer_size must be at least {} bytes, but is {}", MINIMUM_BUFFER_SIZE, value)};
                    }
                    return value;
                });
    },
    uint64_t{4096}};

const ConfigField<bool> NAUTILUS_PERF{
    Identifier::parse("nautilus_perf"),
    "Sets nautilus' perf option: compiled pipelines keep frame pointers, so `perf record -g` can walk through them, and write perf "
    "jitdump records with their symbols, line tables and unwinding information, so that `perf inject --jit` attributes samples to "
    "them. Keeps the optimization level, so the profile shows production code.",
    false};
/// NOLINTEND(bugprone-throwing-static-initialization)

}

Schema<QualifiedErasedConfigField, Ordered> QueryExecutionConfiguration::getConfigSchema()
{
    return createConfigSchema(
        Identifier::parse("default_query_execution"),
        EXECUTION_MODE,
        NAUTILUS_INLINING,
        TUPLE_BUFFER_INLINING,
        PAGE_SIZE,
        NUMBER_OF_PARTITIONS,
        NUMBER_OF_RECORDS_PER_KEY,
        OPERATOR_BUFFER_SIZE,
        NAUTILUS_PERF,
        SliceCacheConfiguration::getConfigSchema(),
        BloomFilterConfiguration::getConfigSchema());
}

QueryExecutionConfiguration QueryExecutionConfiguration::fromConfig(const InstantiatedConfig& config)
{
    const bool nautilusInlining = config.get(NAUTILUS_INLINING);
    if (nautilusInlining && !NES_NAUTILUS_INLINING_ACTIVE)
    {
        NES_WARNING(
            "nautilus_inlining is enabled, but this build did not apply the nautilus inlining pass "
            "(NES_ENABLE_NAUTILUS_INLINING). Proxy functions will not be inlined.");
    }
    return {
        config.get(EXECUTION_MODE),
        nautilusInlining,
        config.get(NUMBER_OF_PARTITIONS),
        config.get(PAGE_SIZE),
        config.get(NUMBER_OF_RECORDS_PER_KEY),
        config.get(OPERATOR_BUFFER_SIZE),
        config.get(TUPLE_BUFFER_INLINING),
        SliceCacheConfiguration::fromConfig(config),
        BloomFilterConfiguration::fromConfig(config),
        config.get(NAUTILUS_PERF)};
}

}
