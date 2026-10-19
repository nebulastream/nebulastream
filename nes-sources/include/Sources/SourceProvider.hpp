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

#include <memory>
#include <string>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <BackpressureChannel.hpp>
#include <QueryId.hpp>

namespace NES
{
class CheckpointManager;

/// Takes a SourceDescriptor and in exchange returns a SourceHandle.
/// The SourceThread spawns an independent thread for data ingestion and it manages the pipeline and task logic.
/// The Source is owned by the SourceThread. The Source ingests bytes from an interface (TCP, CSV, ..) and writes the bytes to a TupleBuffer.
class SourceProvider
{
    size_t defaultMaxInflightBuffers;
    std::shared_ptr<AbstractBufferProvider> bufferPool;
    std::shared_ptr<CheckpointManager> checkpointManager;

public:
    /// Constructor that can be configured with various options
    SourceProvider(size_t defaultMaxInflightBuffers, std::shared_ptr<AbstractBufferProvider> bufferPool);
    /// The sources take part in the checkpoints of the checkpoint manager.
    SourceProvider(
        size_t defaultMaxInflightBuffers,
        std::shared_ptr<AbstractBufferProvider> bufferPool,
        std::shared_ptr<CheckpointManager> checkpointManager);

    /// Returning a shared pointer, because sources may be shared by multiple executable query plans (qeps).
    [[nodiscard]] std::unique_ptr<SourceHandle>
    lower(OriginId originId, BackpressureListener backpressureListener, const SourceDescriptor& sourceDescriptor) const;
    [[nodiscard]] std::unique_ptr<SourceHandle>
    lower(QueryId queryId, OriginId originId, BackpressureListener backpressureListener, const SourceDescriptor& sourceDescriptor) const;

    [[nodiscard]] bool contains(const std::string& sourceType) const;
};

}
