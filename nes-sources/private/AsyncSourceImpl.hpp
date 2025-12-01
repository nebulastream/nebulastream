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


#include "SourceImpl.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <ostream>
#include <stop_token>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

class AsyncSourceImpl final : public SourceImpl
{
public:
    explicit AsyncSourceImpl(
        OriginId originId, /// Todo #241: Rethink use of originId for sources, use new identifier for unique identification.
        std::shared_ptr<AbstractBufferProvider> bufferManager,
        std::unique_ptr<AsyncSource> sourceImplementation)
        : originId(originId)
        , bufferManager(std::move(bufferManager))
        , sourceImplementation(std::move(sourceImplementation))
    {
    }

    /// if not already running, start new thread with runningRoutine (finishes, when runningRoutine finishes)
    [[nodiscard]] bool start(SourceReturnType::EmitFunction&& emitFunction) override;

    /// Blocks the current thread until the source is terminated
    void stop() override { sourceImplementation->close(); }

    /// Attempts to terminate the source within the timeout
    [[nodiscard]] SourceReturnType::TryStopResult tryStop(std::chrono::milliseconds) override
    {
        sourceImplementation->close();
        return SourceReturnType::TryStopResult::SUCCESS;
    }

    friend std::ostream& operator<<(std::ostream& out, const AsyncSourceImpl& impl);

    [[nodiscard]] OriginId getOriginId() const override { return originId; }

    [[nodiscard]] std::ostream& toString(std::ostream&) const override;

protected:
    OriginId originId;
    std::shared_ptr<AbstractBufferProvider> bufferManager;
    std::unique_ptr<AsyncSource> sourceImplementation;
};

}

FMT_OSTREAM(NES::AsyncSourceImpl);
