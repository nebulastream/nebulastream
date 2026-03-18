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
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <TokioSourceRegistry.hpp>

// CXX-generated header for init_source_runtime
#include <nes-source-bindings/lib.h>

namespace NES
{

SourceProvider::SourceProvider(size_t defaultMaxInflightBuffers, std::shared_ptr<AbstractBufferProvider> bufferPool)
    : defaultMaxInflightBuffers(defaultMaxInflightBuffers), bufferPool(std::move(bufferPool))
{
    // Initialize the shared Tokio runtime for Rust async sources.
    // Idempotent (OnceLock) — safe to call multiple times.
    // Uses 2 worker threads; matches the test configuration.
    ::init_source_runtime(2);
}

std::unique_ptr<SourceHandle>
SourceProvider::lower(OriginId originId, BackpressureListener backpressureListener, const SourceDescriptor& sourceDescriptor) const
{
    const auto& sourceType = sourceDescriptor.getSourceType();

    /// The source-specific configuration of maxInflightBuffers takes priority.
    /// If not specified (0), we take the NodeEngine-wide configuration.
    const auto maxInflightBuffers = (sourceDescriptor.getFromConfig(SourceDescriptor::MAX_INFLIGHT_BUFFERS) > 0)
        ? sourceDescriptor.getFromConfig(SourceDescriptor::MAX_INFLIGHT_BUFFERS)
        : defaultMaxInflightBuffers;
    SourceRuntimeConfiguration runtimeConfig{maxInflightBuffers};

    // Check TokioSourceRegistry first — Rust async sources bypass SourceThread entirely.
    if (TokioSourceRegistry::instance().contains(sourceType))
    {
        auto tokioArgs = TokioSourceRegistryArguments{
            sourceDescriptor,
            originId,
            static_cast<uint32_t>(maxInflightBuffers),
            bufferPool
        };
        if (auto tokioSource = TokioSourceRegistry::instance().create(sourceType, std::move(tokioArgs)))
        {
            return std::make_unique<SourceHandle>(std::move(runtimeConfig), std::move(tokioSource.value()));
        }
        throw UnknownSourceType("TokioSourceRegistry contains '{}' but factory returned nullopt", sourceType);
    }

    // Fall through to standard SourceRegistry — C++ sources wrapped in SourceThread.
    /// Todo #241: Get the new source identfier from the source descriptor and pass it to SourceHandle.
    auto sourceArguments = SourceRegistryArguments(sourceDescriptor);
    if (auto source = SourceRegistry::instance().create(sourceType, sourceArguments))
    {
        return std::make_unique<SourceHandle>(
            std::move(backpressureListener), std::move(originId), std::move(runtimeConfig), bufferPool, std::move(source.value()));
    }
    throw UnknownSourceType("unknown source descriptor type: {}", sourceType);
}

bool SourceProvider::contains(const std::string& sourceType) const ///NOLINT(readability-convert-member-functions-to-static)
{
    return SourceRegistry::instance().contains(sourceType)
        || TokioSourceRegistry::instance().contains(sourceType);
}

}
