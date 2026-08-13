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
#include <Util/Variant.hpp>
#include <BloomFilterConfiguration.hpp>
#include <ErrorHandling.hpp>
#include <SliceCacheConfiguration.hpp>

namespace NES
{

namespace
{

/// NOLINTBEGIN(cert-err58-cpp)
const ConfigField<ExecutionMode> EXECUTION_MODE{
    Identifier::parse("execution_mode"),
    "Execution mode for the query compiler"
    "[COMPILER|INTERPRETER].",
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

const ConfigField<uint64_t> PAGE_SIZE{
    Identifier::parse("page_size"),
    "Page size of any other paged data structure",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(narrowConfigValue<int64_t, uint64_t>); },
    uint64_t{1024}};

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
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(narrowConfigValue<int64_t, uint64_t>); },
    uint64_t{4096}};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> QueryExecutionConfiguration::getConfigSchema()
{
    return createConfigSchema(
        Identifier::parse("default_query_execution"),
        EXECUTION_MODE,
        PAGE_SIZE,
        NUMBER_OF_PARTITIONS,
        NUMBER_OF_RECORDS_PER_KEY,
        OPERATOR_BUFFER_SIZE,
        SliceCacheConfiguration::getConfigSchema(),
        BloomFilterConfiguration::getConfigSchema());
}

QueryExecutionConfiguration QueryExecutionConfiguration::fromConfig(const InstantiatedConfig& config)
{
    return {
        config.get(EXECUTION_MODE),
        config.get(NUMBER_OF_PARTITIONS),
        config.get(PAGE_SIZE),
        config.get(NUMBER_OF_RECORDS_PER_KEY),
        config.get(OPERATOR_BUFFER_SIZE),
        SliceCacheConfiguration::fromConfig(config),
        BloomFilterConfiguration::fromConfig(config)};
}

}
