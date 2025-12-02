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

#include <Sources/SourceProvider.hpp>

#include <memory>
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Util/Overloaded.hpp>
#include <AsyncSourceImpl.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include "SourceThread.hpp"

namespace NES
{

SourceProvider::SourceProvider(size_t defaultMaxInflightBuffers, std::shared_ptr<AbstractBufferProvider> bufferPool)
    : defaultMaxInflightBuffers(defaultMaxInflightBuffers), bufferPool(std::move(bufferPool))
{
}

std::unique_ptr<SourceHandle> SourceProvider::lower(OriginId originId, const SourceDescriptor& sourceDescriptor) const
{
    /// Todo #241: Get the new source identfier from the source descriptor and pass it to SourceHandle.
    auto sourceArguments = SourceRegistryArguments(sourceDescriptor);
    if (auto source = SourceRegistry::instance().create(sourceDescriptor.getSourceType(), sourceArguments))
    {
        /// The source-specific configuration of maxInflightBuffers takes priority.
        /// If not specified (0), we take the NodeEngine-wide configuration.
        const auto maxInflightBuffers = (sourceDescriptor.tryGetFromConfig(SourceDescriptor::MAX_INFLIGHT_BUFFERS).value_or(0) > 0)
            ? sourceDescriptor.getFromConfig(SourceDescriptor::MAX_INFLIGHT_BUFFERS)
            : defaultMaxInflightBuffers;
        SourceRuntimeConfiguration runtimeConfig{maxInflightBuffers};

        return std::visit(
            Overloaded{
                [&](std::unique_ptr<Source> source)
                {
                    return std::make_unique<SourceHandle>(
                        runtimeConfig, std::make_unique<SourceThread>(originId, bufferPool, std::move(source)));
                },
                [&](std::unique_ptr<AsyncSource> source)
                {
                    return std::make_unique<SourceHandle>(
                        runtimeConfig, std::make_unique<AsyncSourceImpl>(originId, bufferPool, std::move(source)));
                }},
            std::move(*source));
    }
    throw UnknownSourceType("unknown source descriptor type: {}", sourceDescriptor.getSourceType());
}

bool SourceProvider::contains(const std::string& sourceType) const ///NOLINT(readability-convert-member-functions-to-static)
{
    return SourceRegistry::instance().contains(sourceType);
}

}
