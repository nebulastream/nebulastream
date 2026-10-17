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

#include <Sinks/BackpressureHandler.hpp>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <optional>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/ScopeGuard.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

BackpressureHandler::BackpressureHandler(size_t upperThreshold, size_t lowerThreshold)
    : upperThreshold(upperThreshold), lowerThreshold(lowerThreshold)
{
    if (this->lowerThreshold > this->upperThreshold)
    {
        NES_WARNING("Lower threshold is greater than upper threshold. Setting lower threshold to upper threshold.");
        std::swap(this->lowerThreshold, this->upperThreshold);
    }
}

std::optional<TupleBuffer> BackpressureHandler::onFull(TupleBuffer buffer, BackpressureController& backpressureController)
{
    auto rstate = stateLock.ulock();

    /// If this is the pending retry buffer, re-emit it to keep the retry loop alive.
    if (buffer.getSequenceNumber() == rstate->pendingSequenceNumber && buffer.getChunkNumber() == rstate->pendingChunkNumber)
    {
        return buffer;
    }

    const auto wstate = rstate.moveFromUpgradeToWrite();
    wstate->buffered.emplace_back(std::move(buffer));

    /// Apply backpressure when the buffer count reaches the upper hysteresis threshold.
    if (!wstate->hasBackpressure && wstate->buffered.size() >= upperThreshold)
    {
        backpressureController.applyPressure();
        NES_DEBUG("Backpressure acquired: {} buffered (upper threshold: {})", wstate->buffered.size(), upperThreshold);
        wstate->hasBackpressure = true;
    }

    /// Ensure there is always one pending buffer being retried to avoid deadlocks.
    if (wstate->pendingSequenceNumber == INVALID<SequenceNumber>)
    {
        auto pending = std::move(wstate->buffered.front());
        wstate->buffered.pop_front();
        wstate->pendingSequenceNumber = pending.getSequenceNumber();
        wstate->pendingChunkNumber = pending.getChunkNumber();
        return pending;
    }

    return {};
}

std::optional<TupleBuffer> BackpressureHandler::onSuccess(BackpressureController& backpressureController)
{
    const auto state = stateLock.wlock();
    state->pendingSequenceNumber = INVALID<SequenceNumber>;
    state->pendingChunkNumber = INVALID<ChunkNumber>;

    /// Release backpressure when the buffer count drops to the lower hysteresis threshold.
    if (state->hasBackpressure && state->buffered.size() <= lowerThreshold)
    {
        backpressureController.releasePressure();
        NES_DEBUG("Backpressure released: {} buffered (lower threshold: {})", state->buffered.size(), lowerThreshold);
        state->hasBackpressure = false;
    }

    if (not state->buffered.empty())
    {
        auto nextBuffer = std::move(state->buffered.front());
        state->buffered.pop_front();
        return {nextBuffer};
    }
    return {};
}

std::optional<TupleBuffer>
BackpressureHandler::send(const TupleBuffer& buffer, BackpressureController& backpressureController, const TrySend& trySend)
{
    if (closed)
    {
        NES_WARNING("Sink is closed, dropping buffer: {}-{}", buffer.getSequenceNumber(), buffer.getChunkNumber());
        return {};
    }

    SCOPE_FAIL
    {
        closed = true;
    };
    auto currentBuffer = std::optional(buffer);
    while (currentBuffer)
    {
        if (not trySend(*currentBuffer))
        {
            return onFull(*currentBuffer, backpressureController);
        }
        currentBuffer = onSuccess(backpressureController);
    }
    return {};
}

bool BackpressureHandler::stop(BackpressureController& backpressureController, const TrySend& trySend, const std::function<bool()>& flushed)
{
    if (not closed)
    {
        try
        {
            const auto state = stateLock.wlock();
            auto sent = true;
            while (sent and not state->buffered.empty())
            {
                sent = trySend(state->buffered.front());
                if (sent)
                {
                    state->buffered.pop_front();
                }
            }
            if (not(sent and flushed()))
            {
                return false;
            }
        }
        catch (const Exception& exception)
        {
            NES_WARNING("Sink failed while stopping: {}", exception.what());
        }
    }
    close(backpressureController);
    return true;
}

void BackpressureHandler::close(BackpressureController& backpressureController)
{
    closed = true;
    const auto state = stateLock.wlock();
    if (not state->buffered.empty())
    {
        NES_WARNING("Sink drops {} undelivered buffers", state->buffered.size());
        state->buffered.clear();
    }
    state->pendingSequenceNumber = INVALID<SequenceNumber>;
    state->pendingChunkNumber = INVALID<ChunkNumber>;
    if (state->hasBackpressure)
    {
        backpressureController.releasePressure();
        state->hasBackpressure = false;
    }
}

}
