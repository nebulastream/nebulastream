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

#include <Sinks/SinkProvider.hpp>

#include <memory>
#include <utility>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sinks/TokioSink.hpp>
#include <Util/Logger/Logger.hpp>
#include <nes-sink-validation-bindings/lib.h>
#include <ErrorHandling.hpp>
#include <SinkRegistry.hpp>

namespace NES
{

SinkProvider::SinkProvider(uint32_t defaultChannelCapacity) : defaultChannelCapacity(defaultChannelCapacity)
{
}

std::unique_ptr<Sink> SinkProvider::lower(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor) const
{
    NES_DEBUG("The sinkDescriptor is: {}", sinkDescriptor);
    const auto& sinkType = sinkDescriptor.getSinkType();

    if (exists(sinkType))
    {
        return std::make_unique<TokioSink>(std::move(backpressureController), sinkDescriptor);
    }

    // Fall through to standard SinkRegistry
    auto sinkArguments = SinkRegistryArguments(std::move(backpressureController), sinkDescriptor);
    if (auto sink = SinkRegistry::instance().create(sinkType, std::move(sinkArguments)))
    {
        return std::move(sink.value());
    }
    throw UnknownSinkType("Unknown Sink Descriptor Type: {}", sinkType);
}

bool SinkProvider::contains(const std::string& sinkType) const
{
    return exists(sinkType) || SinkRegistry::instance().contains(sinkType);
}

// Compatibility wrapper -- delegates to SinkProvider with default channel capacity.
std::unique_ptr<Sink> lower(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
{
    static SinkProvider provider;
    return provider.lower(std::move(backpressureController), sinkDescriptor);
}

}
