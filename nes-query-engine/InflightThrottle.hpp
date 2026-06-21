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

#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <stop_token>

namespace NES
{

/// #1713: bounded counter with a dynamically adjustable limit, replacing std::counting_semaphore as the per-source
/// inflight-buffer throttle so the cap can grow/shrink at runtime (adaptive provisioning). With a fixed limit the
/// behaviour is identical to a counting semaphore. condition_variable_any is required to compose with std::stop_callback
/// (std::condition_variable cannot wait on a stop_token).
class InflightThrottle
{
public:
    /// Outcome of an acquire(): whether a slot was taken, and whether the caller had to wait for it (the saturation
    /// signal the adaptive policy grows on).
    struct AcquireResult
    {
        bool acquired;
        bool waited;
    };

    explicit InflightThrottle(const std::size_t initialLimit) : limit(initialLimit) { }

    /// Blocks until inUse < limit OR the stop_token is signalled. Returns {acquired,waited}: acquired=false (and no slot
    /// taken, so the caller must not release()) when it returned due to stop; waited=true if it had to block.
    [[nodiscard]] AcquireResult acquire(const std::stop_token& token)
    {
        std::unique_lock lock(mutex);
        const bool wouldBlock = !(inUse < limit);
        const std::stop_callback callback(token, [this] { cv.notify_all(); });
        cv.wait(lock, [&] { return inUse < limit || token.stop_requested(); });
        if (token.stop_requested())
        {
            return {false, wouldBlock};
        }
        ++inUse;
        return {true, wouldBlock};
    }

    /// Returns one slot. Safe to call from any thread (e.g. the task-completion callback).
    void release()
    {
        bool notify = false;
        {
            const std::lock_guard lock(mutex);
            if (inUse > 0)
            {
                --inUse;
            }
            notify = inUse < limit;
        }
        if (notify)
        {
            cv.notify_one();
        }
    }

    /// Adjust the cap. Growing wakes waiters; shrinking below inUse simply blocks new acquires until releases drain it
    /// -- in-flight buffers are never forcibly reclaimed.
    void setLimit(const std::size_t newLimit)
    {
        bool grew = false;
        {
            const std::lock_guard lock(mutex);
            grew = newLimit > limit;
            limit = newLimit;
        }
        if (grew)
        {
            cv.notify_all();
        }
    }

    [[nodiscard]] std::size_t currentLimit() const
    {
        const std::lock_guard lock(mutex);
        return limit;
    }

    [[nodiscard]] std::size_t currentInUse() const
    {
        const std::lock_guard lock(mutex);
        return inUse;
    }

private:
    mutable std::mutex mutex;
    std::condition_variable_any cv;
    std::size_t inUse = 0;
    std::size_t limit;
};

/// #1713: AIMD policy for the per-source inflight cap. Additive-increase when the source stalls (acquire had to wait,
/// i.e. it is saturated and could use more slots), multiplicative-decrease when it has been idle (slots unused), always
/// clamped to [min, max]. max is the source's configured inflight limit, so the adaptive cap never exceeds today's
/// static cap -- adaptive growth therefore cannot create buffer pressure beyond the non-adaptive baseline. The policy is
/// a pure value object (no locking / no time source) so it can be unit-tested deterministically; the emit loop owns the
/// timing and feeds it the stall signal and elapsed-idle.
struct InflightPolicy
{
    std::size_t min;
    std::size_t max;
    std::size_t current;
    std::size_t additiveStep = 4;
    /// Numerator/denominator of the multiplicative decrease (1/2 by default).
    std::size_t decayNum = 1;
    std::size_t decayDen = 2;

    /// Saturated this round: additively increase, clamped to max.
    std::size_t onStall()
    {
        current = std::min(max, current + additiveStep);
        return current;
    }

    /// Idle long enough: multiplicatively decrease, clamped to min.
    std::size_t onIdleDecay()
    {
        const std::size_t decayed = (current * decayNum) / decayDen;
        current = std::max(min, decayed);
        return current;
    }
};

}
