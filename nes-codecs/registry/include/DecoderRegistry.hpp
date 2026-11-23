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

#include <Codecs/Decoder.hpp>
#include <Codecs/DecoderDescriptor.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

/// Defined in nes-sources, which depends on nes-codecs.
class SourceDescriptor;

/// Called once per source, since decoders keep per-stream state.
using DecoderFactoryFn = std::function<std::unique_ptr<Decoder>(const DecoderDescriptor&, const SourceDescriptor&)>;

class DecoderRegistry : public RuntimeRegistry<DecoderRegistry, std::string, DecoderFactoryFn, /*CaseSensitive*/ false>
{
public:
    static DecoderRegistry& instance();
};

/// The config was built by the codec's DecoderConfigRegistry entry, so the cast is safe.
template <typename DecoderImpl, typename ConfigStruct>
DecoderFactoryFn makeDecoderFactory()
{
    return [](const DecoderDescriptor& descriptor, const SourceDescriptor& sourceDescriptor) -> std::unique_ptr<Decoder>
    {
        const auto& config = descriptor.getConfig().getAs<ConfigStruct>();
        if constexpr (std::constructible_from<DecoderImpl, const ConfigStruct&, const SourceDescriptor&>)
        {
            return std::make_unique<DecoderImpl>(config, sourceDescriptor);
        }
        else
        {
            return std::make_unique<DecoderImpl>(config);
        }
    };
}

}
