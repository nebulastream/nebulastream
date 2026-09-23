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
#include <optional>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <BackpressureChannel.hpp>

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
    const BufferId bufferId{buffer.getOriginId(), buffer.getSequenceNumber(), buffer.getChunkNumber()};

    /// If this is the pending retry buffer, re-emit it to keep the retry loop alive.
    if (rstate->pending == bufferId)
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
    if (not wstate->pending)
    {
        auto pending = std::move(wstate->buffered.front());
        wstate->buffered.pop_front();
        wstate->pending = BufferId{pending.getOriginId(), pending.getSequenceNumber(), pending.getChunkNumber()};
        return pending;
    }

    return {};
}

std::optional<TupleBuffer> BackpressureHandler::onSuccess(const TupleBuffer& buffer, BackpressureController& backpressureController)
{
    const auto state = stateLock.wlock();
    const BufferId bufferId{buffer.getOriginId(), buffer.getSequenceNumber(), buffer.getChunkNumber()};
    if (state->pending != bufferId)
    {
        return {};
    }
    state->pending.reset();

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
        state->pending = BufferId{nextBuffer.getOriginId(), nextBuffer.getSequenceNumber(), nextBuffer.getChunkNumber()};
        return {nextBuffer};
    }
    return {};
}

bool BackpressureHandler::empty() const
{
    return stateLock.rlock()->buffered.empty();
}

}
