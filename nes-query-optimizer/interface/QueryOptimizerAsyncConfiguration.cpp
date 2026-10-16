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

#include <QueryOptimizerAsyncConfiguration.hpp>

#include <cstdint>
#include <expected>
#include <optional>
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

/// An unset value falls back to the default of the Async source and the Handoff sink, so these fields are optional (an
/// explicit value is forwarded to their configs, absence is not).
std::expected<std::optional<uint64_t>, Exception> optionalUInt(const ConfigLiteral& literal)
{
    return tryGetOr<int64_t>(literal, expectedType<uint64_t>())
        .and_then(narrowConfigValue<int64_t, uint64_t>)
        .transform([](const uint64_t value) { return std::optional{value}; });
}

/// NOLINTBEGIN(bugprone-throwing-static-initialization,cert-err58-cpp)
const ConfigField<std::optional<uint64_t>> MAX_CONCURRENCY{
    Identifier::parse("max_concurrency"),
    "Rows of one async stage whose calls are in flight at the same time",
    optionalUInt,
    std::nullopt,
    "Not set"};

const ConfigField<std::optional<uint64_t>> CHANNEL_CAPACITY{
    Identifier::parse("channel_capacity"),
    "Buffers that wait between the producer half of an async stage and its calls before the producer is held back",
    optionalUInt,
    std::nullopt,
    "Not set"};

const ConfigField<std::optional<uint64_t>> MAX_RETRIES{
    Identifier::parse("max_retries"),
    "Further attempts of a failed async call, the n-th after 100 ms * 2^n",
    optionalUInt,
    std::nullopt,
    "Not set"};
/// NOLINTEND(bugprone-throwing-static-initialization,cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> QueryOptimizerAsyncConfiguration::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("async"), MAX_CONCURRENCY, CHANNEL_CAPACITY, MAX_RETRIES);
}

QueryOptimizerAsyncConfiguration QueryOptimizerAsyncConfiguration::fromConfig(const InstantiatedConfig& config)
{
    return {config.get(MAX_CONCURRENCY), config.get(CHANNEL_CAPACITY), config.get(MAX_RETRIES)};
}

}
