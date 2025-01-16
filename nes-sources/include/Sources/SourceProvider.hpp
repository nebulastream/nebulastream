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
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>

namespace NES::Sources
{

/// Takes a SourceDescriptor and in exchange returns a SourceHandle.
/// The SourceThread spawns an independent thread for data ingestion and it manages the pipeline and task logic.
/// The Source is owned by the SourceThread. The Source ingests bytes from an interface (TCP, CSV, ..) and writes the bytes to a TupleBuffer.
class SourceProvider
{
    /// Todo #237: reevaluate whether we still need num source local buffers, and potentially use new configuration approach.
    static constexpr int NUM_SOURCE_LOCAL_BUFFERS = 64;

public:
    SourceProvider() = default;
    static std::unique_ptr<SourceProvider> create();

    /// Returning a shared pointer, because sources may be shared by multiple executable query plans (qeps).
    static std::unique_ptr<SourceHandle>
    lower(OriginId originId, const SourceDescriptor& sourceDescriptor, std::shared_ptr<NES::Memory::AbstractPoolProvider> bufferPool);

    ~SourceProvider() = default;
};

}
