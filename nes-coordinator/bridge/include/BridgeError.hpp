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
#include <string>
#include <type_traits>
#include <utility>

#include <cpptrace/from_current.hpp>
#include <nes-coordinator-bridge/error.h>
#include <rust/cxx.h>
#include <ErrorHandling.hpp>

namespace NES::Bridge
{

/// Codes that report a condition that the caller expects and acts on, rather than a fault in the worker.
/// They are the answer to a question, so nobody debugs them and a trace would go unused.
constexpr bool isExpectedCondition(const ErrorCode code)
{
    return code == ErrorCode::QueryNotFound;
}

/// Converts an exception for Rust with its code and stacktrace intact.
/// A thrown exception would reach Rust as a message alone, because that is all cxx transports.
/// The code fits in `uint16_t` since the highest defined one is 10000.
/// Zero is never assigned to an exception, which is what lets zero mean "no error" on the Rust side.
///
/// Only a fault gets a trace.
/// Resolving one reads the DWARF tables of the whole binary, so it is far too expensive to pay on a polled call,
/// and it is the one part of this that can fail on its own.
inline BridgeError make_error(const Exception& ex)
{
    auto trace = isExpectedCondition(ex.code()) ? rust::String() : rust::String(ex.trace().to_string());
    return {.code = static_cast<uint16_t>(ex.code()), .msg = rust::String(std::string(ex.what())), .trace = std::move(trace)};
}

/// Whether the call that returned it succeeded.
inline bool isNone(const BridgeError& error)
{
    return error.code == 0;
}

/// Raises the failure that a call reported alongside its payload, as the exception for that code.
/// A caller or a test can then act on the code instead of on a message.
inline void raiseReported(const BridgeError& error)
{
    if (!isNone(error))
    {
        throw Exception(std::string{error.msg}, error.code);
    }
}

/// Runs a call that can fail by returning an error or by throwing, and reports both in the answer's error field.
/// A call that answers nothing but success returns the error alone.
/// The generated cxx shims are `noexcept`, so an escaping exception would terminate the process.
template <typename Call>
auto guard(Call&& call)
{
    using Answer = typename std::invoke_result_t<Call>::value_type;
    constexpr bool answersNothing = std::is_void_v<Answer>;
    CPPTRACE_TRY
    {
        auto result = std::forward<Call>(call)();
        if constexpr (answersNothing)
        {
            return result ? BridgeError{} : make_error(result.error());
        }
        else
        {
            if (result)
            {
                return std::move(*result);
            }
            Answer failed{};
            failed.error = make_error(result.error());
            return failed;
        }
    }
    CPPTRACE_CATCH(...)
    {
        /// An NES exception keeps its own code and trace.
        /// Anything else, including an error raised by a Rust callback, arrives as UnknownException with its message.
        if constexpr (answersNothing)
        {
            return make_error(wrapExternalException());
        }
        else
        {
            Answer failed{};
            failed.error = make_error(wrapExternalException());
            return failed;
        }
    }
    std::unreachable();
}

}
