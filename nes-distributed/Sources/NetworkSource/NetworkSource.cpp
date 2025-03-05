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

#include <chrono>
#include <memory>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <network/lib.h>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

#include "Bridge.hpp"
#include "Runtime/AbstractBufferProvider.hpp"

namespace NES::Sources
{

NetworkSource::NetworkSource(const SourceDescriptor& sourceDescriptor)
    : channelIdentifier(sourceDescriptor.getFromConfig(ConfigParametersNetwork::CHANNEL)), receiverServer(receiver_instance())
{
}

std::ostream& NetworkSource::toString(std::ostream& str) const
{
    return str << fmt::format("NetworkSource({})", this->channelIdentifier);
}

void NetworkSource::open(std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider)
{
    this->bufferProvider = std::move(bufferProvider);
    this->channel = register_receiver_channel(*receiverServer, rust::String(channelIdentifier));
}

size_t NetworkSource::fillTupleBuffer(Memory::TupleBuffer& tupleBuffer, const std::stop_token&)
{
    TupleBufferBuilder builder(tupleBuffer, *bufferProvider);
    /// TODO #695 We can use the stop token to interrupt the receive!
    if (receive_buffer(**channel, builder))
    {
        return 1; /// Received one buffer
    }
    else
    {
        return 0; /// End of Stream
    }
}


void NetworkSource::close()
{
    INVARIANT(channel.has_value(), "Network Source was closed multiple times");
    close_receiver_channel(std::move(*channel));
}


std::unique_ptr<SourceRegistryReturnType> SourceGeneratedRegistrar::RegisterNetworkSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<NetworkSource>(sourceRegistryArguments.sourceDescriptor);
}

}
