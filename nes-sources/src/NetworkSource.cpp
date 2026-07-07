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

#include <Sources/NetworkSource.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <utility>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Configurations/Validation/EndpointValidation.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <Util/Variant.hpp>
#include <fmt/format.h>
#include <network/lib.h>
#include <rust/cxx.h>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

/// All config fields of the network source, shared by getConfigSchema (declaration) and
/// NetworkSourceConfig::fromConfig (typed extraction). Constructed lazily on first use so no
/// exception can escape static initialization.
struct NetworkConfigFields
{
    ConfigField<std::string> channel;
    ConfigField<std::string> bind;
    ConfigField<size_t> receiverQueueSize;
};

const NetworkConfigFields& configFields()
{
    static const NetworkConfigFields fields{
        .channel
        = {"CHANNEL",
           [](const ConfigLiteral& literal)
           {
               return NES::tryGetOr<std::string>(literal, expectedType<std::string>())
                   .and_then(
                       [](const std::string& value) -> std::expected<std::string, Exception>
                       {
                           if (!stringToUUID(value))
                           {
                               return std::unexpected{
                                   InvalidConfigParameter("NetworkSource: channel must be a valid UUID, got: {}", value)};
                           }
                           return value;
                       });
           }},
        .bind
        = {"BIND",
           [](const ConfigLiteral& literal)
           {
               return NES::tryGetOr<std::string>(literal, expectedType<std::string>())
                   .and_then(
                       [](const std::string& value) -> std::expected<std::string, Exception>
                       {
                           if (!EndpointValidation{}.isValid(value))
                           {
                               return std::unexpected{
                                   InvalidConfigParameter("NetworkSource: bind must be host:port format, got: {}", value)};
                           }
                           return value;
                       });
           }},
        /// Per-channel receiver queue size override. 0 means use the worker-level default.
        /// When a user explicitly sets receiver_queue_size=0, the lambda rejects it with an error.
        /// The default value (0) is returned directly by the config system, bypassing the lambda.
        .receiverQueueSize
        = {"RECEIVER_QUEUE_SIZE",
           [](const ConfigLiteral& literal)
           {
               /// Integer literals are always passed down signed; lower into size_t.
               return NES::tryGetOr<int64_t>(literal, expectedType<size_t>())
                   .and_then(downcastConfigValue<int64_t, size_t>)
                   .and_then(
                       [](const size_t value) -> std::expected<size_t, Exception>
                       {
                           if (value == 0)
                           {
                               return std::unexpected{
                                   InvalidConfigParameter("NetworkSource: receiver_queue_size must be > 0 when explicitly set")};
                           }
                           return value;
                       });
           },
           size_t{0}},
    };
    return fields;
}

}

Schema<QualifiedErasedConfigField, Ordered> NetworkSource::getConfigSchema()
{
    const auto& fields = configFields();
    return createConfigSchema(Identifier::parse("NETWORK_SOURCE"), fields.channel, fields.bind, fields.receiverQueueSize);
}

NetworkSourceConfig NetworkSourceConfig::fromConfig(const InstantiatedConfig& config)
{
    const auto& fields = configFields();
    return NetworkSourceConfig{
        .channel = config.get(fields.channel),
        .bind = config.get(fields.bind),
        .receiverQueueSize = config.get(fields.receiverQueueSize)};
}

NetworkSource::NetworkSource(const NetworkSourceConfig& config)
    : channelId(config.channel), receiverQueueSize(config.receiverQueueSize), receiverServer(receiver_instance(config.bind))
{
}

std::ostream& NetworkSource::toString(std::ostream& str) const
{
    return str << fmt::format("NetworkSource({})", channelId);
}

void NetworkSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    this->bufferProvider = std::move(provider);
    const NetworkServiceOptions options{
        .sender_queue_size = 0,
        .max_pending_acks = 0,
        .receiver_queue_size = static_cast<uint32_t>(receiverQueueSize),
    };
    this->channel = register_receiver_channel(*receiverServer, rust::String(channelId), options);
    NES_DEBUG("Receiver channel registered: {}", channelId);
}

Source::FillTupleBufferResult NetworkSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    PRECONDITION(channel, "Network Source was opened multiple times");
    PRECONDITION(bufferProvider, "Network Source was opened without a buffer provider");
    TupleBufferBuilder builder(tupleBuffer, *bufferProvider);

    /// If the source is requested to shutdown the network channel is closed, which will interrupt the call to receive_buffer.
    const std::stop_callback callback(stopToken, [this] { interrupt_receive(**channel); });

    if (receive_buffer(**channel, builder))
    {
        return FillTupleBufferResult::withBytes(tupleBuffer.getNumberOfTuples()); /// Received one buffer
    }

    /// Receive Buffer has failed, which means that the queue was closed.
    /// The SourceThread logic will figure out if the queue was closed by an external source (i.e. the other side of the network connection)
    /// or because of a stop_request.
    return FillTupleBufferResult::eos(); /// End of Stream
}

void NetworkSource::close()
{
    PRECONDITION(channel.has_value(), "Network Source was closed multiple times or never opened");
    close_receiver_channel(std::move(*channel));
    NES_DEBUG("Receiver channel closed: {}", channelId);
}

}
