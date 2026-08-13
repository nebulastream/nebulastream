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

#pragma once

#include <concepts>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/RuntimeRegistry.hpp>
#include <BackpressureChannel.hpp>

namespace NES
{

using SinkRegistryReturnType = std::unique_ptr<Sink>;

struct SinkRegistryArguments
{
    BackpressureController backpressureController;
    SinkDescriptor sinkDescriptor;
};

using SinkFactoryFn = std::function<SinkRegistryReturnType(SinkRegistryArguments)>;

/// Factory for the common case: the descriptor's type-erased plugin data is the sink's own
/// config struct (put there by this sink's SinkConfigRegistry entry), so the any_cast is safe.
/// Sinks that need more than their config (e.g. the sink schema) can additionally take the
/// descriptor.
template <typename SinkImpl, typename ConfigStruct>
SinkFactoryFn makeSinkFactory()
{
    return [](SinkRegistryArguments arguments) -> SinkRegistryReturnType
    {
        auto config = arguments.sinkDescriptor.getPluginData().getAs<ConfigStruct>();
        if constexpr (std::constructible_from<SinkImpl, BackpressureController, const ConfigStruct&, const SinkDescriptor&>)
        {
            return std::make_unique<SinkImpl>(std::move(arguments.backpressureController), std::move(config), arguments.sinkDescriptor);
        }
        else
        {
            return std::make_unique<SinkImpl>(std::move(arguments.backpressureController), std::move(config));
        }
    };
}

class SinkRegistry : public RuntimeRegistry<SinkRegistry, std::string, SinkFactoryFn, /*CaseSensitive*/ false>
{
public:
    static SinkRegistry& instance();
};

}
