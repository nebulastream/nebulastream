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

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceThreadHandle.hpp>
#include "nes-source-validation-bindings/lib.h"

#include "TokioSource.hpp"

namespace NES
{

SourceProvider::SourceProvider(size_t defaultMaxInflightBuffers, std::shared_ptr<AbstractBufferProvider> bufferPool)
    : defaultMaxInflightBuffers(defaultMaxInflightBuffers), bufferPool(std::move(bufferPool))
{
}

std::unique_ptr<SourceHandle>
SourceProvider::lower(OriginId originId, BackpressureListener backpressureListener, const SourceDescriptor& sourceDescriptor) const
{
    const auto& sourceType = sourceDescriptor.getSourceType();

    SourceRuntimeConfiguration runtimeConfig{64};

    if (source_exists(sourceType))
    {
        return std::make_unique<TokioSource>(
            std::move(backpressureListener), sourceDescriptor, originId, bufferPool, std::move(runtimeConfig));
    }

    // Fall through to standard SourceRegistry — C++ sources wrapped in SourceThread.
    /// Todo #241: Get the new source identfier from the source descriptor and pass it to SourceHandle.
    auto sourceArguments = SourceRegistryArguments(sourceDescriptor);
    if (auto source = SourceRegistry::instance().create(sourceType, sourceArguments))
    {
        return std::make_unique<SourceThreadHandle>(
            std::move(backpressureListener), std::move(originId), std::move(runtimeConfig), bufferPool, std::move(source.value()));
    }
    throw UnknownSourceType("unknown source descriptor type: {}", sourceType);
}

bool SourceProvider::contains(const std::string& sourceType) const ///NOLINT(readability-convert-member-functions-to-static)
{
    return SourceRegistry::instance().contains(sourceType);
}

}
