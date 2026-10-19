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
#include <optional>
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/SourceCheckpointing.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <SourceRegistry.hpp>

namespace NES
{

SourceProvider::SourceProvider(size_t defaultMaxInflightBuffers, std::shared_ptr<AbstractBufferProvider> bufferPool)
    : defaultMaxInflightBuffers(defaultMaxInflightBuffers), bufferPool(std::move(bufferPool))
{
}

SourceProvider::SourceProvider(
    size_t defaultMaxInflightBuffers,
    std::shared_ptr<AbstractBufferProvider> bufferPool,
    std::shared_ptr<CheckpointManager> checkpointManager)
    : defaultMaxInflightBuffers(defaultMaxInflightBuffers)
    , bufferPool(std::move(bufferPool))
    , checkpointManager(std::move(checkpointManager))
{
}

std::unique_ptr<SourceHandle>
SourceProvider::lower(OriginId originId, BackpressureListener backpressureListener, const SourceDescriptor& sourceDescriptor) const
{
    return lower(QueryId::invalid(), originId, std::move(backpressureListener), sourceDescriptor);
}

std::unique_ptr<SourceHandle> SourceProvider::lower(
    QueryId queryId, OriginId originId, BackpressureListener backpressureListener, const SourceDescriptor& sourceDescriptor) const
{
    /// Todo #241: Get the new source identfier from the source descriptor and pass it to SourceHandle.
    if (const auto sourceFactory = SourceRegistry::instance().find(sourceDescriptor.getSourceType()))
    {
        auto source = (*sourceFactory)(SourceRegistryArguments{sourceDescriptor});
        /// The source-specific configuration of maxInflightBuffers takes priority.
        /// If not specified (0), we take the NodeEngine-wide configuration.
        const auto maxInflightBuffers = (sourceDescriptor.getFromConfig(SourceDescriptor::MAX_INFLIGHT_BUFFERS) > 0)
            ? sourceDescriptor.getFromConfig(SourceDescriptor::MAX_INFLIGHT_BUFFERS)
            : defaultMaxInflightBuffers;
        SourceRuntimeConfiguration runtimeConfig{maxInflightBuffers};

        std::optional<SourceCheckpointing> checkpointing;
        if (checkpointManager and queryId.getLocalQueryId() != INVALID_LOCAL_QUERY_ID)
        {
            checkpointing = SourceCheckpointing{.checkpointManager = checkpointManager, .queryId = std::move(queryId)};
        }
        return std::make_unique<SourceHandle>(
            std::move(backpressureListener),
            std::move(originId),
            std::move(runtimeConfig),
            bufferPool,
            std::move(source),
            std::move(checkpointing));
    }
    throw UnknownSourceType("unknown source descriptor type: {}", sourceDescriptor.getSourceType());
}

bool SourceProvider::contains(const std::string& sourceType) const ///NOLINT(readability-convert-member-functions-to-static)
{
    return SourceRegistry::instance().contains(sourceType);
}

}
