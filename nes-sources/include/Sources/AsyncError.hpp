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
#include <expected>
#include <ErrorHandling.hpp>

namespace NES
{
/// Operational outcome of an async invocation. Retry policy never depends on a thrown exception.
struct AsyncError
{
    enum class Type : uint8_t
    {
        Permanent,
        Retryable,
        Cancelled,
        TimedOut,
    };

    Exception exception;
    Type kind = Type::Permanent;

    static AsyncError cancelled() { return {.exception = IOError("Async operation cancelled"), .kind = Type::Cancelled}; }

    static AsyncError timedOut() { return {.exception = IOError("Async operation timed out"), .kind = Type::TimedOut}; }
};

template <typename T>
using AsyncResult = std::expected<T, AsyncError>;
}
