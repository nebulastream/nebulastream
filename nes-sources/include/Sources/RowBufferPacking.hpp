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
#include <chrono>
#include <cstddef>
#include <iterator>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

constexpr std::chrono::milliseconds ROW_POP_TIMEOUT{10};
constexpr char ROW_TUPLE_DELIMITER = '\n';

template <typename NextRow>
Source::FillTupleBufferResult packRowsIntoBuffer(
    TupleBuffer& tupleBuffer,
    const std::stop_token& stopToken,
    const std::chrono::milliseconds flushInterval,
    std::optional<std::string>& pendingRow,
    size_t& emittedRows,
    NextRow&& nextRow,
    const std::string_view rowOrigin)
{
    const auto available = tupleBuffer.getAvailableMemoryArea<char>();
    const auto deadline = std::chrono::steady_clock::now() + flushInterval;
    size_t written = 0;

    while (not stopToken.stop_requested())
    {
        if (not pendingRow.has_value())
        {
            auto popTimeout = ROW_POP_TIMEOUT;
            if (written > 0)
            {
                const auto remaining = deadline - std::chrono::steady_clock::now();
                if (remaining <= decltype(remaining)::zero())
                {
                    break;
                }
                popTimeout = std::min(ROW_POP_TIMEOUT, std::chrono::ceil<std::chrono::milliseconds>(remaining));
            }
            pendingRow = nextRow(popTimeout);
        }

        if (pendingRow.has_value())
        {
            if (const size_t rowSize = pendingRow->size() + 1; rowSize <= available.size() - written)
            {
                std::ranges::copy(*pendingRow, std::next(available.begin(), static_cast<ptrdiff_t>(written)));
                written += pendingRow->size();
                available[written++] = ROW_TUPLE_DELIMITER;
                ++emittedRows;
                pendingRow.reset();
            }
            else if (written > 0)
            {
                break;
            }
            else
            {
                NES_WARNING(
                    "Dropping a row of {} bytes from {}, it does not fit into a TupleBuffer of {} bytes",
                    pendingRow->size() + 1,
                    rowOrigin,
                    available.size());
                pendingRow.reset();
            }
        }

        if (written > 0 && std::chrono::steady_clock::now() >= deadline)
        {
            break;
        }
    }

    if (written == 0)
    {
        return Source::FillTupleBufferResult::eos();
    }
    return Source::FillTupleBufferResult::withBytes(written);
}

}
