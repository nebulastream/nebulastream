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

#include <QueryOptimizerNetworkConfiguration.hpp>

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

/// An unset override falls back to the worker-level network default, so these fields are optional
/// (an explicit value is forwarded to the network source/sink config, absence is not).
std::expected<std::optional<uint64_t>, Exception> optionalUInt(const ConfigLiteral& literal)
{
    return tryGetOr<int64_t>(literal, expectedType<uint64_t>())
        .and_then(narrowConfigValue<int64_t, uint64_t>)
        .transform([](const uint64_t value) { return std::optional{value}; });
}

/// NOLINTBEGIN(cert-err58-cpp)
const ConfigField<std::optional<uint64_t>> SENDER_QUEUE_SIZE{
    Identifier::parse("sender_queue_size"),
    "Size of the sender software queue per network channel.",
    optionalUInt,
    std::nullopt,
    "Not set"};

const ConfigField<std::optional<uint64_t>> MAX_PENDING_ACKS{
    Identifier::parse("max_pending_acks"),
    "Maximum number of in-flight buffers awaiting acknowledgment per network channel",
    optionalUInt,
    std::nullopt,
    "Not set"};

const ConfigField<std::optional<uint64_t>> RECEIVER_QUEUE_SIZE{
    Identifier::parse("receiver_queue_size"), "Size of the receiver data queue per network channel", optionalUInt, std::nullopt, "Not set"};

const ConfigField<std::optional<uint64_t>> BACKPRESSURE_UPPER_THRESHOLD{
    Identifier::parse("backpressure_upper_threshold"),
    "Number of buffered tuples at which backpressure is acquired per network channel",
    optionalUInt,
    std::nullopt,
    "Not set"};

const ConfigField<std::optional<uint64_t>> BACKPRESSURE_LOWER_THRESHOLD{
    Identifier::parse("backpressure_lower_threshold"),
    "Number of buffered tuples at which backpressure is released per network channel",
    optionalUInt,
    std::nullopt,
    "Not set"};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> QueryOptimizerNetworkConfiguration::getConfigSchema()
{
    return createConfigSchema(
        Identifier::parse("network"),
        SENDER_QUEUE_SIZE,
        MAX_PENDING_ACKS,
        RECEIVER_QUEUE_SIZE,
        BACKPRESSURE_UPPER_THRESHOLD,
        BACKPRESSURE_LOWER_THRESHOLD);
}

QueryOptimizerNetworkConfiguration QueryOptimizerNetworkConfiguration::fromConfig(const InstantiatedConfig& config)
{
    return {
        config.get(SENDER_QUEUE_SIZE),
        config.get(MAX_PENDING_ACKS),
        config.get(RECEIVER_QUEUE_SIZE),
        config.get(BACKPRESSURE_UPPER_THRESHOLD),
        config.get(BACKPRESSURE_LOWER_THRESHOLD)};
}

}
