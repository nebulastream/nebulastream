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
#include <string>

#include <coordinator/lib.h>
#include <rust/cxx.h>
#include <ErrorHandling.hpp>

namespace NES
{

/// Codes that report a condition the caller expects and acts on, rather than a fault in the worker. They are the
/// answer to a question, so no one debugs them and the trace is dead weight.
constexpr bool isExpectedCondition(const ErrorCode code)
{
    return code == ErrorCode::QueryNotFound;
}

/// Carries an exception to Rust with its code and stacktrace intact. A thrown exception would reach Rust as a message
/// alone, because that is all cxx transports. The code stays inside `uint16_t` since the highest defined one is 10000,
/// and zero is never assigned to an exception, which is what lets zero mean "no error" on the Rust side.
///
/// Only a fault carries a trace. Resolving one reads the DWARF tables of the whole binary, so it is far too expensive
/// to pay on a polled call, and it is the one part of this that can fail on its own.
inline BridgeError make_error(const Exception& ex)
{
    auto trace = isExpectedCondition(ex.code()) ? rust::String() : rust::String(ex.trace().to_string());
    return {.code = static_cast<uint16_t>(ex.code()), .msg = rust::String(std::string(ex.what())), .trace = std::move(trace)};
}

}
