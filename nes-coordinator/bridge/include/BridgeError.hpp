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

/// Converts an exception into the error struct that crosses the FFI boundary, with its code and stack trace.
/// A thrown exception would reach Rust as a message only.
/// Code 0 means no error, and no exception has that code.
inline BridgeError toBridgeError(const Exception& exception)
{
    return {
        .code = static_cast<uint16_t>(exception.code()),
        .msg = rust::String{std::string{exception.what()}},
        .trace = rust::String{exception.trace().to_string()}};
}

/// Whether the call that returned this error struct succeeded.
inline bool isOk(const BridgeError& error)
{
    return error.code == 0;
}

/// Throws the error that a Rust call reported, as an exception with the same code.
/// The stack trace from Rust is not kept.
inline void throwIfError(const BridgeError& error)
{
    if (!isOk(error))
    {
        throw Exception{std::string{error.msg}, error.code};
    }
}

namespace detail
{
/// The value that a call returns on success: `T` for `T` and for `std::expected<T, Exception>`.
template <typename Returned>
struct SuccessValue
{
    using type = Returned;
};

template <typename T>
struct SuccessValue<std::expected<T, Exception>>
{
    using type = T;
};
}

/// Runs a call and converts a returned error or a thrown exception into the error field of the result.
/// The call may return a result struct, a `std::expected`, or nothing.
/// A call without a result returns only the error struct.
/// No exception may leave an FFI function, because the functions that cxx generates are `noexcept`.
template <typename Call>
auto catchAsBridgeError(Call&& call)
{
    using Returned = std::invoke_result_t<Call>;
    using Result = detail::SuccessValue<Returned>::type;
    constexpr bool returnsVoid = std::is_void_v<Result>;
    constexpr bool returnsExpected = !std::is_same_v<Returned, Result>;
    CPPTRACE_TRY
    {
        if constexpr (!returnsExpected)
        {
            if constexpr (returnsVoid)
            {
                std::forward<Call>(call)();
                return BridgeError{};
            }
            else
            {
                return std::forward<Call>(call)();
            }
        }
        else
        {
            auto result = std::forward<Call>(call)();
            if constexpr (returnsVoid)
            {
                return result ? BridgeError{} : toBridgeError(result.error());
            }
            else
            {
                if (result)
                {
                    return std::move(*result);
                }
                Result failed{};
                failed.error = toBridgeError(result.error());
                return failed;
            }
        }
    }
    CPPTRACE_CATCH(...)
    {
        /// An NES exception keeps its code and trace.
        /// Any other exception, including an error thrown by a Rust callback, is reported as UnknownException with its message.
        if constexpr (returnsVoid)
        {
            return toBridgeError(wrapExternalException());
        }
        else
        {
            Result failed{};
            failed.error = toBridgeError(wrapExternalException());
            return failed;
        }
    }
    std::unreachable();
}

}
