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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include <UdfBackend.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

/// The UDFs of one UDF operator, all served by the same bridge library. They run as one unit per row, under one acquisition
/// of whatever the bridge serializes on (the GIL for CPython) instead of one per UDF.
///
/// Memory layout (all buffers are flat, the UDFs follow each other in the order they were passed in):
///   - `argValues`/`argLens`/`argNulls`: as in UdfBackend, with the arguments of all UDFs one after the other.
///   - `results`: one 8-byte slot per UDF. Receives a scalar result. For a VARSIZED result the slot instead holds, on input, a
///     pointer to a buffer of MAX_VARSIZED_RESULT_BYTES.
///   - `resultLens`: one 8-byte slot per UDF, receiving the length of a VARSIZED result.
///   - `resultNulls`: one byte per UDF, non-zero if its result is NULL.
class UdfStage
{
public:
    static constexpr std::size_t SLOT_BYTES = 8;
    /// Cap on a VARSIZED result. A larger result fails the query rather than corrupting memory.
    static constexpr std::uint64_t MAX_VARSIZED_RESULT_BYTES = 8192;

    explicit UdfStage(const std::vector<UdfDescriptor>& udfs);

    /// Throws UdfExecutionError on a recoverable failure of a UDF.
    void executeRow(
        const std::int8_t* argValues,
        const std::int8_t* argLens,
        const std::int8_t* argNulls,
        std::int8_t* results,
        std::int8_t* resultLens,
        std::int8_t* resultNulls);

private:
    struct Udf
    {
        std::shared_ptr<UdfBackend> backend;
        std::size_t firstArgument;
        bool varsizedResult;
    };

    std::vector<Udf> udfs;
    /// Taken before the bridge's lock: a thread that holds the bridge's lock and waits for a backend's lock would otherwise
    /// deadlock with a thread that holds the backend's lock and waits for the bridge's. Nothing but this stage uses its backends.
    std::mutex mutex;
};

}
