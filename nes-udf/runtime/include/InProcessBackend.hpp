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
#include <mutex>
#include <vector>

#include <UdfBackend.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

/// In-process backend: `dlopen`s the registered `.so` in the worker and calls the UDF C ABI directly.
/// Recoverable UDF failures throw UdfExecutionError; a fatal crash in the `.so` takes down the worker
/// (the documented v1 limitation closed by a future SidecarBackend).
class InProcessBackend final : public UdfBackend
{
public:
    explicit InProcessBackend(const UdfDescriptor& descriptor);
    ~InProcessBackend() override;
    InProcessBackend(const InProcessBackend&) = delete;
    InProcessBackend& operator=(const InProcessBackend&) = delete;
    InProcessBackend(InProcessBackend&&) = delete;
    InProcessBackend& operator=(InProcessBackend&&) = delete;

    void executeScalarRow(
        const std::int8_t* argValues,
        const std::int8_t* argLens,
        const std::int8_t* argNulls,
        std::int8_t* resultScalar,
        int* resultNull) override;

    std::uint64_t executeVarsizedRow(
        const std::int8_t* argValues,
        const std::int8_t* argLens,
        const std::int8_t* argNulls,
        std::int8_t* resultBuffer,
        std::uint64_t maxResultLen,
        int* resultNull) override;

private:
    /// Builds the ABI per-argument arrays from the staged buffers and calls execute_udf_row, throwing
    /// UdfExecutionError on a recoverable failure. Caller holds `mutex`.
    void invokeUdf(
        const std::int8_t* argValues,
        const std::int8_t* argLens,
        const std::int8_t* argNulls,
        std::int8_t* resultScalar,
        char** resultString,
        long long* resultStringLen,
        int* resultNull);

    void* libHandle = nullptr;
    void* executeFn = nullptr; /// execute_udf_row
    void* cleanupFn = nullptr; /// cleanup_udf
    int udfHandle = -1;
    std::size_t argCount = 0;
    std::vector<int> argTypeCodes; /// per argument; used to tell VARSIZED slots (pointers) from scalar slots (values)

    /// Reusable per-call scratch (guarded by `mutex`): the ABI's per-argument pointer/length/null arrays.
    std::vector<const void*> argPointers;
    std::vector<long long> argLengths;
    std::vector<int> argNullFlags;

    /// The bridge serializes internally (GIL), but a mutex keeps a non-thread-safe `.so` correct too.
    std::mutex mutex;
};

}
