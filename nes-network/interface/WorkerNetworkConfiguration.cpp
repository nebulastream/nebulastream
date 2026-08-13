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

#include <WorkerNetworkConfiguration.hpp>

#include <cstdint>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Variant.hpp>

namespace NES
{

namespace
{

/// NOLINTBEGIN(cert-err58-cpp)
const ConfigField<uint64_t> SENDER_QUEUE_SIZE{
    Identifier::parse("sender_queue_size"),
    "Default size of the sender software queue per network channel. May be overridden per NetworkSink.",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
    uint64_t{1024}};

const ConfigField<uint64_t> MAX_PENDING_ACKS{
    Identifier::parse("max_pending_acks"),
    "Default maximum number of in-flight buffers awaiting acknowledgment per network channel. May be overridden per NetworkSink.",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
    uint64_t{64}};

const ConfigField<uint64_t> RECEIVER_QUEUE_SIZE{
    Identifier::parse("receiver_queue_size"),
    "Default size of the receiver data queue per network channel. May be overridden per NetworkSource.",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
    uint64_t{10}};

const ConfigField<uint64_t> SENDER_IO_THREADS{
    Identifier::parse("sender_io_threads"),
    "Number of IO threads for the sender network runtime. 0 means use the number of available cores.",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
    uint64_t{1}};

const ConfigField<uint64_t> RECEIVER_IO_THREADS{
    Identifier::parse("receiver_io_threads"),
    "Number of IO threads for the receiver network runtime. 0 means use the number of available cores.",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
    uint64_t{1}};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> WorkerNetworkConfiguration::getConfigSchema()
{
    return createConfigSchema(
        Identifier::parse("network"), SENDER_QUEUE_SIZE, MAX_PENDING_ACKS, RECEIVER_QUEUE_SIZE, SENDER_IO_THREADS, RECEIVER_IO_THREADS);
}

WorkerNetworkConfiguration WorkerNetworkConfiguration::fromConfig(const InstantiatedConfig& config)
{
    return {
        .senderQueueSize = config.get(SENDER_QUEUE_SIZE),
        .maxPendingAcks = config.get(MAX_PENDING_ACKS),
        .receiverQueueSize = config.get(RECEIVER_QUEUE_SIZE),
        .senderIOThreads = config.get(SENDER_IO_THREADS),
        .receiverIOThreads = config.get(RECEIVER_IO_THREADS)};
}
}
