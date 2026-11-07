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
#include <vector>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Strings.hpp>
#include <Util/Variant.hpp>
#include <fmt/format.h>
#include <BloomFilterConfiguration.hpp>
#include <ErrorHandling.hpp>
#include <SliceCacheConfiguration.hpp>

namespace NES
{

namespace
{

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

const ConfigField<std::vector<std::string>> PYTHON_UDF_IMPORT_PATHS{
    Identifier::parse("python_udf_import_paths"),
    "Comma-separated directories from which inline Python UDFs may import user-provided modules",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<std::string>(literal, expectedType<std::string>())
            .transform([](const std::string& value) { return splitWithStringDelimiter<std::string>(value, ","); });
    },
    std::vector<std::string>{},
    ""};

const ConfigField<bool> NAUTILUS_PERF{
    Identifier::parse("nautilus_perf"),
    "Sets nautilus' perf option: compiled pipelines keep frame pointers, so `perf record -g` can walk through them, and write perf "
    "jitdump records with their symbols, line tables and unwinding information, so that `perf inject --jit` attributes samples to "
    "them. Keeps the optimization level, so the profile shows production code.",
    false};
const ConfigField<InProcessUdfCalls> IN_PROCESS_UDF_CALLS{
    Identifier::parse("in_process_udf_calls"),
    fmt::format("How IN_PROCESS UDFs are called: per record (ROW), or per batch of records in Arrow format through pyarrow (BATCH): {}", enumPipeList<InProcessUdfCalls>()),
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<std::string>(literal, expectedType<std::string>())
            .and_then(
                [](std::string value) -> std::expected<InProcessUdfCalls, Exception>
                {
                    if (const auto parsed = EnumWrapper{value}.asEnum<InProcessUdfCalls>())
                    {
                        return *parsed;
                    }
                    return std::unexpected{InvalidConfigParameter("Invalid in_process_udf_calls, must be one of {}: {}", enumPipeList<InProcessUdfCalls>(), value)};
                });
    },
    InProcessUdfCalls::ROW,
    "ROW"};

const ConfigField<InProcessUdfInterpreter> IN_PROCESS_UDF_INTERPRETER{
    Identifier::parse("in_process_udf_interpreter"),
    fmt::format("Which interpreter a worker thread runs IN_PROCESS UDFs in: the shared one with its one GIL (SHARED), a subinterpreter of the thread with its own GIL, for UDFs without extension modules (SUBINTERPRETER), or a copy of the bridge and interpreter in a link namespace of the thread (DLMOPEN): {}", enumPipeList<InProcessUdfInterpreter>()),
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<std::string>(literal, expectedType<std::string>())
            .and_then(
                [](std::string value) -> std::expected<InProcessUdfInterpreter, Exception>
                {
                    if (const auto parsed = EnumWrapper{value}.asEnum<InProcessUdfInterpreter>())
                    {
                        return *parsed;
                    }
                    return std::unexpected{InvalidConfigParameter("Invalid in_process_udf_interpreter, must be one of {}: {}", enumPipeList<InProcessUdfInterpreter>(), value)};
                });
    },
    InProcessUdfInterpreter::SHARED,
    "SHARED"};

const ConfigField<bool> IN_PROCESS_UDF_FUSION{
    Identifier::parse("in_process_udf_fusion"),
    "Fuses the IN_PROCESS UDFs of an operator, such as both of `SELECT f(a), g(b)`, into one function, so that a record or batch "
    "needs one call into the language for all of them instead of one per UDF.",
    false};

/// NOLINTEND(bugprone-throwing-static-initialization)

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
        PYTHON_UDF_IMPORT_PATHS,
        NAUTILUS_PERF,
        IN_PROCESS_UDF_CALLS,
        IN_PROCESS_UDF_INTERPRETER,
        IN_PROCESS_UDF_FUSION,
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
        BloomFilterConfiguration::fromConfig(config),
        config.get(PYTHON_UDF_IMPORT_PATHS),
        config.get(NAUTILUS_PERF),
        config.get(IN_PROCESS_UDF_CALLS),
        config.get(IN_PROCESS_UDF_INTERPRETER),
        config.get(IN_PROCESS_UDF_FUSION)};
}

}
