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
#include <memory>
#include <string>
#include <vector>

#include <Model.hpp>

namespace NES
{

/// Wraps the inference runtime. Owns the compiled model, its input/output byte
/// buffers and the cached function handle. One wrapper is consumed by exactly
/// one worker thread at a time (no internal synchronization). The implementation
/// is pimpl'd so consumers of this header don't pull in the runtime backend.
class InferenceRuntime
{
public:
    InferenceRuntime();
    ~InferenceRuntime();

    InferenceRuntime(const InferenceRuntime&) = delete;
    InferenceRuntime& operator=(const InferenceRuntime&) = delete;
    InferenceRuntime(InferenceRuntime&&) noexcept;
    InferenceRuntime& operator=(InferenceRuntime&&) noexcept;

    /// Create the runtime session from the compiled model and allocate the input
    /// and output byte buffers. Must be called exactly once.
    void setup(const CompiledModel& model);

    /// Run inference: read from the owned input buffer, write into the owned output buffer.
    void infer();

    /// Raw byte pointer into the input / output buffer. Callers (nautilus-generated
    /// code) write/read directly instead of going through per-element helpers.
    [[nodiscard]] std::byte* getInputData() { return inputData.get(); }

    [[nodiscard]] std::byte* getOutputData() { return outputData.get(); }

    [[nodiscard]] size_t getInputSize() const { return inputSize; }

    [[nodiscard]] size_t getOutputSize() const { return outputSize; }

private:
    struct Impl;
    std::unique_ptr<Impl> impl;

    std::vector<size_t> inputShape;
    size_t nDim = 0;
    std::string functionName;

    /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
    std::unique_ptr<std::byte[]> inputData;
    size_t inputSize = 0;
    /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
    std::unique_ptr<std::byte[]> outputData;
    size_t outputSize = 0;
};

}
