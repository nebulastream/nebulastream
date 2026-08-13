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
#include <Configurations/ConfigValue.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
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
                [](std::string&& value) -> std::expected<ExecutionMode, Exception>
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
    "Partitions in a hash table",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
    uint64_t{100}};

const ConfigField<uint64_t> PAGE_SIZE{
    Identifier::parse("page_size"),
    "Page size of any other paged data structure",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
    uint64_t{1024}};

const ConfigField<uint64_t> NUMBER_OF_RECORDS_PER_KEY{
    Identifier::parse("number_of_records_per_key"),
    "Expected number of records per key, for example in a hash join. If set too low or high affects the performance.",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
    uint64_t{10}};

const ConfigField<uint64_t> MAX_NUMBER_OF_BUCKETS{
    Identifier::parse("max_number_of_buckets"),
    "Maximal number of buckets for a hash table. If set too low or high degrades either the performance or increases the memory usage.",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
    uint64_t{10'000}};

const ConfigField<uint64_t> OPERATOR_BUFFER_SIZE{
    Identifier::parse("operator_buffer_size"),
    "Buffer size of a operator e.g. during scan",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
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
        MAX_NUMBER_OF_BUCKETS,
        OPERATOR_BUFFER_SIZE,
        SliceCacheConfiguration::getConfigSchema(),
        BloomFilterConfiguration::getConfigSchema());
}

QueryExecutionConfiguration QueryExecutionConfiguration::fromConfig(const InstantiatedConfig& config)
{
    return {
        .executionMode = config.get(EXECUTION_MODE),
        .numberOfPartitions = config.get(NUMBER_OF_PARTITIONS),
        .pageSize = config.get(PAGE_SIZE),
        .numberOfRecordsPerKey = config.get(NUMBER_OF_RECORDS_PER_KEY),
        .maxNumberOfBuckets = config.get(MAX_NUMBER_OF_BUCKETS),
        .operatorBufferSize = config.get(OPERATOR_BUFFER_SIZE),
        .sliceCacheConfiguration = SliceCacheConfiguration::fromConfig(config),
        .bloomFilterConfiguration = BloomFilterConfiguration::fromConfig(config)};
}

}
