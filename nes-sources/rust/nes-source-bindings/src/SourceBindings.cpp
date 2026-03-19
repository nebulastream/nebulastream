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

#include <SourceBindings.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <stop_token>
#include <utility>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/Logger.hpp>

// --- C-linkage callback implementations ---

uint8_t bridge_emit(
    void* context,
    uint64_t origin_id,
    void* buffer_ptr,
    uintptr_t semaphore_ptr)
{
    auto* ctx = static_cast<NES::EmitContext*>(context);
    auto* rawBuffer = static_cast<NES::TupleBuffer*>(buffer_ptr);

    NES_DEBUG("bridge_emit called: origin_id={} buffer_ptr={}", origin_id, buffer_ptr);

    // Null buffer pointer signals End-of-Stream from the Rust source task.
    // Sent through the bridge channel (same path as data) to guarantee ordering:
    // all data buffers are processed before EOS.
    if (rawBuffer == nullptr)
    {
        NES_DEBUG("bridge_emit: sending EoS for origin_id={}", origin_id);
        ctx->emitFunction(
            NES::OriginId(origin_id),
            NES::SourceReturnType::EoS{},
            ctx->stopSource.get_token()
        );
        // Release the emitFunction to drop captured shared_ptr refs to pipeline nodes.
        ctx->emitFunction = {};
        ctx->eosProcessed.store(true, std::memory_order_release);
        return 0;
    }

    // Copy the buffer -- the copy constructor calls retain(), giving C++ shared
    // ownership. When the Rust TupleBufferHandle drops (calling release) and the
    // C++ pipeline finishes (destructor calls release), refcount reaches 0 -> recycled.
    NES::TupleBuffer buffer(*rawBuffer);

    // Set buffer metadata -- same fields that SourceThread::addBufferMetaData sets.
    buffer.setOriginId(NES::OriginId(origin_id));
    buffer.setCreationTimestampInMS(NES::Timestamp(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
    buffer.setSequenceNumber(NES::SequenceNumber(ctx->sequenceNumber++));
    buffer.setChunkNumber(NES::ChunkNumber(1));
    buffer.setLastChunk(true);

    NES_DEBUG("bridge_emit: emitting buffer for origin_id={} seq={} numTuples={}", origin_id, ctx->sequenceNumber - 1, buffer.getNumberOfTuples());

    // Set onComplete callback to release the Rust semaphore slot when the
    // pipeline finishes processing this buffer. The semaphore_ptr is an
    // Arc<Semaphore> raw pointer created via Arc::into_raw() in Rust's emit().
    std::function<void()> onComplete;
    if (semaphore_ptr != 0)
    {
        onComplete = [semaphore_ptr] { nes_release_semaphore_slot(reinterpret_cast<const void*>(semaphore_ptr)); };
    }

    auto result = ctx->emitFunction(
        NES::OriginId(origin_id),
        NES::SourceReturnType::Data{std::move(buffer), std::move(onComplete)},
        ctx->stopSource.get_token()
    );

    NES_DEBUG("bridge_emit: emitFunction returned {} for origin_id={}", static_cast<int>(result), origin_id);
    return static_cast<uint8_t>(result);
}

void on_source_error_callback(
    void* context,
    uint64_t source_id,
    const char* message)
{
    auto* ctx = static_cast<NES::ErrorContext*>(context);
    NES_ERROR("TokioSource {} (ctx sourceId={}): source error: {}",
              source_id, ctx->sourceId, message);
}
