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
#include <cstring>
#include <string_view>
#include <system_error>

#include <Runtime/TupleBuffer.hpp>
#include <Util/Strings.hpp>
#include <fast_float/fast_float.h>
#include <ErrorHandling.hpp>

namespace NES
{

/// Hardcoded fast numeric parse for the uncompiled input path -- this deliberately does NOT go through the
/// UncompiledInputParser plugin registry (a per-field std::function call is a real cost on the hottest loop,
/// and the registry only exposes the slow Default* / std::from_chars parsers). fast_float is the same core
/// the compiled FastINT parser uses: an integer `from_chars` overload for integrals and the Eisel-Lemire
/// algorithm for floating point (== EiselFloatF64), a single direct call, no thread_local, no proxy.
///
/// fast_float stops at the first non-digit and still reports success, so the full-field consumption check
/// (`ptr == end`) is MANDATORY -- otherwise "12x" would parse as 12. Narrow integer types are range-checked
/// by fast_float (e.g. "300" into int8_t -> errc).
template <typename T>
[[nodiscard]] T uncompiledFastParse(const std::string_view fieldValue)
{
    T result;
    const char* const begin = fieldValue.data();
    const char* const end = begin + fieldValue.size();
    if (auto [ptr, ec] = fast_float::from_chars(begin, end, result); ec == std::errc() && ptr == end)
    {
        return result;
    }
    throw CannotFormatMalformedStringValue("Could not parse '{}' in the uncompiled fast parser", fieldValue);
}

/// Original/naive numeric parse: the codebase's from_chars_with_exception -- integers via std::from_chars,
/// floats via std::stod (trim + std::string alloc + exceptions), i.e. EXACTLY the codec the registry
/// Default* parsers wrap. It is a direct, hardcoded call (no std::function / registry indirection), so a
/// fast-vs-slow delta on the input side is the parser codec alone. (libc++ has no floating-point
/// std::from_chars, which is why fast_float / stod are the two real choices here.)
template <typename T>
[[nodiscard]] T uncompiledSlowParse(const std::string_view fieldValue)
{
    return from_chars_with_exception<T>(fieldValue);
}

/// Codec-selected numeric parse leaf. `useSlow` (resolved once per task from NES_INPUT_CODEC) picks the
/// original std::from_chars codec; otherwise fast_float. Both are hardcoded, inlinable calls -- the branch
/// is on a per-task-constant bool, so the naive baseline pays no dispatch overhead the fast path avoids.
template <typename T>
[[nodiscard]] T uncompiledParse(const std::string_view fieldValue, const bool useSlow)
{
    return useSlow ? uncompiledSlowParse<T>(fieldValue) : uncompiledFastParse<T>(fieldValue);
}

/// Write a fixed-size value into the formatted tuple buffer at the given byte offset (row-wise layout).
template <typename T>
void uncompiledWriteFixed(const T value, const size_t writeOffsetInBytes, TupleBuffer& formattedBuffer)
{
    std::memcpy(formattedBuffer.getAvailableMemoryArea().data() + writeOffsetInBytes, &value, sizeof(T));
}

}
