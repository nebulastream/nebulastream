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

#include <Sources/TokioSource.hpp>
#include <TokioSourceImpl.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <ostream>
#include <utility>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/Logger.hpp>

// CXX-generated header for the Phase 2 bridge (ffi_source module in nes-source-lib)
#include <nes-source-lib/src/lib.h>

// IoBindings.hpp provides EmitContext, ErrorContext, bridge_emit, on_source_error_callback
#include <IoBindings.hpp>

namespace NES
{

TokioSource::TokioSource(
    OriginId originId,
    uint32_t inflightLimit,
    std::shared_ptr<AbstractBufferProvider> bufferProvider,
    SpawnFn spawnFn)
    : originId(std::move(originId))
    , inflightLimit(inflightLimit)
    , bufferProvider(std::move(bufferProvider))
    , spawnFn(std::move(spawnFn))
{
}

TokioSource::~TokioSource()
{
    if (started && !stopped)
    {
        stop();
    }
    // rustHandle (rust::Box<SourceHandle>) is dropped automatically by unique_ptr destructor,
    // which invokes RustHandleImpl destructor, which drops the rust::Box.
}

bool TokioSource::start(SourceReturnType::EmitFunction&& emitFunction)
{
    if (started)
    {
        NES_ERROR("TokioSource {}: start() called but already started", originId);
        return false;
    }

    try
    {
        // Create EmitContext: holds the emit function and a stop_source for stop tokens
        emitContext = std::unique_ptr<EmitContext>(new EmitContext{
            std::move(emitFunction),
            std::stop_source{},
            1,
            {}
        });

        // Create ErrorContext: holds source_id for error callback logging
        errorContext = std::make_unique<ErrorContext>(ErrorContext{
            originId.getRawValue()
        });

        // Call the SpawnFn which invokes the appropriate Rust FFI entry point.
        rustHandle = spawnFn(
            originId.getRawValue(),
            reinterpret_cast<uintptr_t>(bufferProvider.get()),
            inflightLimit,
            reinterpret_cast<uintptr_t>(bridge_emit),
            reinterpret_cast<uintptr_t>(emitContext.get()),
            reinterpret_cast<uintptr_t>(on_source_error_callback),
            reinterpret_cast<uintptr_t>(errorContext.get())
        );

        started = true;

        NES_DEBUG("TokioSource {}: started successfully", originId);
        return true;
    }
    catch (const std::exception& e)
    {
        NES_ERROR("TokioSource {}: start() failed: {}", originId, e.what());
        emitContext.reset();
        errorContext.reset();
        return false;
    }
}

void TokioSource::stop()
{
    if (!started || stopped)
    {
        return;
    }

    NES_DEBUG("TokioSource {}: stop requested", originId);

    // Call Rust FFI stop_source -- non-blocking, cancels CancellationToken
    ::stop_source(*rustHandle->handle);

    // Also request_stop on the C++ side stop_source (for the stop_token used in bridge_emit)
    if (emitContext)
    {
        emitContext->stopSource.request_stop();
    }

    stopped = true;
    NES_DEBUG("TokioSource {}: stop completed", originId);
}

SourceReturnType::TryStopResult TokioSource::tryStop(const std::chrono::milliseconds /*timeout*/)
{
    // Request cooperative stop via CancellationToken (idempotent).
    stop();

    // Wait for the bridge thread to process EOS before returning SUCCESS.
    // This mirrors SourceThread::tryStop() which joins the thread before SUCCESS,
    // ensuring the emitFunction lambda (and its captured successor shared_ptrs)
    // has been cleared. Without this, the captured refs prevent the pipeline
    // node custom deleter from firing, hanging the query lifecycle.
    if (emitContext && emitContext->eosProcessed.load(std::memory_order_acquire))
    {
        return SourceReturnType::TryStopResult::SUCCESS;
    }
    return SourceReturnType::TryStopResult::TIMEOUT;
}

OriginId TokioSource::getSourceId() const
{
    return originId;
}

std::ostream& operator<<(std::ostream& out, const TokioSource& source)
{
    return out << "TokioSource{originId=" << source.originId << "}";
}

} // namespace NES
