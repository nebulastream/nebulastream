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

#include "NetworkSource.hpp"

#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <network/lib.h>
#include <rust/cxx.h>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

NetworkSource::NetworkSource(const SourceDescriptor& sourceDescriptor)
    : channelId(sourceDescriptor.getFromConfig(ConfigParametersNetworkSource::CHANNEL))
    , receiverServer(receiver_instance(sourceDescriptor.getFromConfig(ConfigParametersNetworkSource::BIND)))
{
}

std::ostream& NetworkSource::toString(std::ostream& str) const
{
    return str << fmt::format("NetworkSource({})", channelId);
}

void NetworkSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    this->bufferProvider = std::move(provider);
    this->channel = register_receiver_channel(*receiverServer, rust::String(channelId));
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
        return FillTupleBufferResult(tupleBuffer.getNumberOfTuples()); /// Received one buffer
    }

    /// Receive Buffer has failed, which means that the queue was closed.
    /// The SourceThread logic will figure out if the queue was closed by an external source (i.e. the other side of the network connection)
    /// or because of a stop_request.
    return {}; /// End of Stream
}

void NetworkSource::close()
{
    PRECONDITION(channel.has_value(), "Network Source was closed multiple times or never opened");
    close_receiver_channel(std::move(*channel));
    NES_DEBUG("Receiver channel closed: {}", channelId);
}

DescriptorConfig::Config NetworkSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersNetworkSource>(std::move(config), name());
}

SourceValidationRegistryReturnType RegisterNetworkSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return NetworkSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterNetworkSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<NetworkSource>(sourceRegistryArguments.sourceDescriptor);
}

}
