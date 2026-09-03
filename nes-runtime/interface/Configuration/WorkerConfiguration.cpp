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

#include <Configuration/WorkerConfiguration.hpp>

#include <bit>
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
#include <Util/DumpMode.hpp>
#include <Util/Variant.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <QueryEngineConfiguration.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <WorkerNetworkConfiguration.hpp>

namespace NES
{

namespace
{

/// NOLINTBEGIN(cert-err58-cpp)
const ConfigField<uint64_t> TOTAL_MEMORY_IN_BYTES{
    Identifier::parse("total_memory_in_bytes"),
    "Total memory budget in bytes for the global buffer pool (pooled + unpooled).",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<int64_t>(literal, expectedType<uint64_t>())
            .and_then(narrowConfigValue<int64_t, uint64_t>)
            .and_then(
                [](const uint64_t value) -> std::expected<uint64_t, Exception>
                {
                    if (value == 0)
                    {
                        return std::unexpected{InvalidConfigParameter("total_memory_in_bytes must not be zero")};
                    }
                    return value;
                });
    },
    uint64_t{268435456}};

const ConfigField<float> UNPOOLED_MEMORY_FRACTION{
    Identifier::parse("unpooled_memory_fraction"),
    "Fraction (0.0-1.0) of total memory reserved for unpooled buffers.",
    [](const ConfigLiteral& literal)
    {
        return tryGetDoubleOrInt(literal, expectedType<double>())
            .and_then(
                [](const double value) -> std::expected<float, Exception>
                {
                    if (value < 0.0 || value > 1.0)
                    {
                        return std::unexpected{InvalidConfigParameter("Value {} out of range [0, 1]", value)};
                    }
                    return static_cast<float>(value);
                });
    },
    0.5F};

const ConfigField<uint64_t> BUFFER_ALIGNMENT_IN_BYTES{
    Identifier::parse("buffer_alignment_in_bytes"),
    "Byte alignment of every buffer (power of two, <= page size).",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<int64_t>(literal, expectedType<uint64_t>())
            .and_then(narrowConfigValue<int64_t, uint64_t>)
            .and_then(
                [](const uint64_t value) -> std::expected<uint64_t, Exception>
                {
                    if (!std::has_single_bit(value))
                    {
                        return std::unexpected{InvalidConfigParameter("Buffer alignment must be a power of two, but was: {}", value)};
                    }
                    return value;
                });
    },
    uint64_t{64}};

const ConfigField<uint64_t> DEFAULT_MAX_INFLIGHT_BUFFERS{
    Identifier::parse("default_max_inflight_buffers"),
    "Number of buffers a source can have inflight before blocking. May be overwritten by a source-specific configuration (see "
    "SourceDescriptor).",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(narrowConfigValue<int64_t, uint64_t>); },
    uint64_t{64}};

const ConfigField<DumpMode::Options> DUMP_COMPILATION_RESULT{
    Identifier::parse("dump_compilation_result"),
    fmt::format("If and where to dump query compilation results: {}", enumPipeList<DumpMode::Options>()),
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<std::string>(literal, expectedType<std::string>())
            .and_then(
                [](std::string value) -> std::expected<DumpMode::Options, Exception>
                {
                    if (const auto parsed = EnumWrapper{value}.asEnum<DumpMode::Options>())
                    {
                        return *parsed;
                    }
                    return std::unexpected{
                        InvalidConfigParameter("Invalid dump mode, must be one of {}: {}", enumPipeList<DumpMode::Options>(), value)};
                });
    },
    DumpMode::Options::NONE,
    "NONE"};

const ConfigField<bool> DUMP_GRAPH{Identifier::parse("dump_graph"), "If to dump graph of the compilation results", false};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> WorkerConfiguration::getConfigSchema()
{
    return createConfigSchema(
        Identifier::parse("worker"),
        QueryEngineConfiguration::getConfigSchema(),
        QueryExecutionConfiguration::getConfigSchema(),
        WorkerNetworkConfiguration::getConfigSchema(),
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT_IN_BYTES,
        DEFAULT_MAX_INFLIGHT_BUFFERS,
        DUMP_COMPILATION_RESULT,
        DUMP_GRAPH);
}

WorkerConfiguration WorkerConfiguration::fromConfig(const InstantiatedConfig& config)
{
    return {
        QueryEngineConfiguration::fromConfig(config),
        QueryExecutionConfiguration::fromConfig(config),
        WorkerNetworkConfiguration::fromConfig(config),
        config.get(TOTAL_MEMORY_IN_BYTES),
        config.get(UNPOOLED_MEMORY_FRACTION),
        config.get(BUFFER_ALIGNMENT_IN_BYTES),
        config.get(DEFAULT_MAX_INFLIGHT_BUFFERS),
        config.get(DUMP_COMPILATION_RESULT),
        config.get(DUMP_GRAPH)};
}
}
