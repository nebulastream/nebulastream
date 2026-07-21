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

#include <any>
#include <concepts>
#include <functional>
#include <memory>
#include <string>

#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

/// Maps a source type name to a factory that instantiates the source from a SourceDescriptor.
/// Entries self-register at static initialization time via generated glue TUs
/// (see create_runtime_registry(Source ...) in nes-sources/CMakeLists.txt).
using SourceFactoryFn = std::function<std::unique_ptr<Source>(const SourceDescriptor&)>;

class SourceRegistry : public RuntimeRegistry<SourceRegistry, std::string, SourceFactoryFn, /*CaseSensitive*/ false>
{
};

/// Factory for the common case: the descriptor's type-erased plugin data is the source's own
/// config struct (put there by this source's SourceConfigRegistry entry), so the any_cast is safe.
/// Sources that need more than their config can additionally take the descriptor.
template <typename SourceImpl, typename ConfigStruct>
SourceFactoryFn makeSourceFactory()
{
    return [](const SourceDescriptor& descriptor) -> std::unique_ptr<Source>
    {
        auto config = descriptor.getPluginData().getAs<ConfigStruct>();
        if constexpr (std::constructible_from<SourceImpl, const ConfigStruct&, const SourceDescriptor&>)
        {
            return std::make_unique<SourceImpl>(std::move(config), descriptor);
        }
        else
        {
            return std::make_unique<SourceImpl>(std::move(config));
        }
    };
}

}
