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
#include <span>
#include <stop_token>
#include <string_view>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>

namespace NES
{

/// Intermediate base for sources that produce an opaque byte stream parsed downstream by an InputFormatter.
///
/// Some formatters (notably simdjson) read past the logical end of their input as part of SIMD lookahead.
/// `RawSource` reserves the formatter's required tail padding inside the TupleBuffer and exposes only the
/// remaining prefix as a `span<std::byte>` to the implementation. The reserved tail stays addressable
/// (not poisoned under ASan) so SIMD over-reads land on valid bytes.
///
/// Sources that own a TupleBuffer's layout directly (e.g. NetworkSource, which receives already-formed
/// buffers from another worker) should keep deriving from `Source` and override `fillTupleBuffer`.
class RawSource : public Source
{
public:
    /// Padding bytes that the input format identified by `inputFormatterType` may over-read past
    /// the content end. Keep in sync with the InputFormatter implementations.
    /// Returns 0 for formatters with no over-read.
    static std::size_t requiredTailPaddingFor(std::string_view inputFormatterType) noexcept;

    explicit RawSource(std::size_t tailPadding) noexcept;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) final;

protected:
    /// Implementations write at most `out.size()` bytes into `out`. The TupleBuffer holds an additional
    /// `tailPadding` bytes past `out.end()` that are reserved for downstream parser lookahead and MUST NOT
    /// be written by the source.
    virtual FillTupleBufferResult fillRaw(std::span<std::byte> out, const std::stop_token& stopToken) = 0;

private:
    std::size_t tailPadding;
};

}
