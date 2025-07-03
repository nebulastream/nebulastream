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
#include <cstddef>
#include <string>
#include <string_view>

namespace NES::Sources
{

static constexpr size_t STASH_TARGET_CAPACITY = 1024;

/// A simple stash for persisting partial payloads that did not fit into a TB between calls to `fillTupleBuffer`.
/// When a payload does not fit, stash the remainder.
/// On the next invocation, consume the stashed payload before reading new data.
/// The stash can be in two states:
/// 1. Empty: consumed == tail == 0
/// 2. Partly Consumed: consumed < tail
///
/// The stash has the following two operations:
/// 1. Stash a new payload by placing it into the internal buffer. This operation is allowed only when the stash is in empty state.
/// It will copy the underlying data into the buffer to ensure lifetime until the consume operation occurs.
/// 2. Consume up to `numBytes` from the stashed payload. This operation is allowed only when the stash is in partly consumed state.
/// This will return a view into the underlying buffer, meaning the contents will need to be copied out into a target buffer
/// to ensure proper lifetime.
///
/// The consume method takes care of transferring the stash into the empty state when the whole payload has been consumed.
/// The caller has to take care of consuming before stashing more data.
/// @note This object is not thread-safe.
class PayloadStash
{
public:
    PayloadStash() { buf.reserve(STASH_TARGET_CAPACITY); }
    ~PayloadStash() = default;

    /// No copying or moving necessary.
    [[nodiscard]] PayloadStash(const PayloadStash&) = delete;
    PayloadStash(PayloadStash&&) = delete;
    PayloadStash& operator=(const PayloadStash&) = delete;
    PayloadStash& operator=(PayloadStash&&) = delete;

    /// When the tail is 0, everything has been consumed
    [[nodiscard]] bool empty() const noexcept { return tail == 0; }

    /// Consume up to `numBytes` from the stashed payload.
    /// This is either the capacity of a TB, or the remaining part of the stashed payload, whatever is smaller.
    /// @return a view to the consumed payload
    [[nodiscard]] std::string_view consume(size_t numBytes)
    {
        INVARIANT(consumed < tail, "Cannot consume more data.");

        numBytes = std::min(numBytes, tail - consumed);
        const size_t consumedUpTo = consumed;
        consumed += numBytes;

        /// If we consumed the whole payload, reset the stash
        if (consumed == tail)
        {
            reset();
        }
        return std::string_view{buf}.substr(consumedUpTo, numBytes);
    }

    /// Stash a new payload by copying it into the internal buffer
    /// Reset the consumed pointer and set the tail pointer to the end of the payload
    void stash(const std::string_view payload)
    {
        INVARIANT(consumed == 0 && tail == 0, "Consume stash before stashing a new payload.");

        buf.assign(payload);
        /// Exclusive end of viable data that is ready to be consumed
        tail = payload.size();

        /// If the last payload increased over the default capacity, shrink it back
        if (buf.capacity() > STASH_TARGET_CAPACITY)
        {
            buf.shrink_to_fit();
        }
    }

private:
    std::string buf;
    size_t consumed = 0;
    size_t tail = 0;

    void reset() noexcept
    {
        consumed = 0;
        tail = 0;
    }
};

}
