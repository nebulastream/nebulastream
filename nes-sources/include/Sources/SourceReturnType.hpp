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

#include <cstdint>
#include <functional>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES::SourceReturnType
{
/// Todo #237: Improve error handling in sources
struct Error
{
    Exception ex;
};

struct Data
{
    TupleBuffer buffer;
    /// Optional per-buffer callback invoked when the pipeline finishes processing
    /// this buffer. Used by sources to implement inflight limiting (semaphore release).
    std::function<void()> onComplete;
};

struct EoS
{
};

enum class TryStopResult : uint8_t
{
    SUCCESS,
    TIMEOUT,
    NOT_RUNNING /// Thread was already stopped or never started
};

enum class EmitResult : uint8_t
{
    SUCCESS,
    STOP_REQUESTED,
};

enum class AsyncEmitCompletionResult : uint8_t
{
    /// Async Operation has been completed successfully
    SUCCESS,
};

enum class AsyncEmitResult : uint8_t
{
    /// Async Operation succeeded without blocking
    SUCCESS,
    /// Async Operation blocked. The callback was registered and will be invoked once the operation is completed
    CALLBACK_REGISTERED,
    /// Async Operation blocked. The callback could not be registered. The operation has to be retried.
    RETRY,
};

using SourceReturnType = std::variant<Error, Data, EoS>;
using EmitFunction = std::function<EmitResult(OriginId, SourceReturnType, const std::stop_token&)>;

using AsyncOperationCallback = std::function<void(AsyncEmitCompletionResult)>;
using AsyncEmitFunction = std::function<AsyncEmitResult(OriginId, SourceReturnType, AsyncOperationCallback)>;

}
