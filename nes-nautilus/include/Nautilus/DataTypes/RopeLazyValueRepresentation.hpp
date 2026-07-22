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
#include <optional>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/LazyValueRepresentation.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Arena.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{
class VarVal;

/// A lazy VARSIZED value modelled as a rope: an ordered list of byte spans (see LazyValueRepresentation::Span)
/// materialised into a contiguous buffer only on demand.
///
/// Span-composing string operations (v1: CONCAT) build a rope instead of eagerly allocating an arena buffer and
/// copying every operand into it. The output formatter then writes each span straight into the output buffer, so
/// the payload is copied ONCE (source -> output) instead of twice (source -> arena -> output). This is the
/// structural fix for the CONCAT double-copy that LLVM provably cannot remove (a multi-source fill never
/// split-forwards across the seam of two partial writes).
///
/// v1 scope is the serialize path. Every OTHER consumer (arithmetic, equality, store-to-window-state,
/// getRawValueAs<VariableSizedData>) routes through the virtual parseValue()/getContent() below, which
/// materialise once (memoised) into a contiguous VariableSizedData -- exactly the eager body CONCAT used to run,
/// now deferred and only paid when something genuinely needs contiguous bytes.
class RopeLazyValueRepresentation final : public LazyValueRepresentation
{
public:
    /// @param spans      the ordered segments (host-time list; view-spans borrow the input buffer / const globals).
    /// @param totalSize  Σ segment lengths (the logical string length; already null-adjusted by the builder).
    /// @param arena      captured arena handle used only if a fallback materialisation is ever required.
    RopeLazyValueRepresentation(
        std::vector<Span> spans,
        const nautilus::val<uint64_t>& totalSize,
        const nautilus::val<Arena*>& arena,
        const DataType& type,
        const nautilus::val<bool>& isNull);

    /// Copy-free: hand back the segment list so the formatter can walk it. Never materialises.
    [[nodiscard]] std::vector<Span> getSpans() const override { return spans; }

    [[nodiscard]] bool isRope() const override { return true; }

    /// Fallback path: materialise (memoised) and return the contiguous buffer pointer.
    [[nodiscard]] nautilus::val<int8_t*> getContent() const override;

    /// Fallback path: materialise (memoised) into a contiguous VariableSizedData.
    [[nodiscard]] VarVal parseValue() const override;

private:
    /// Allocate Σlen from the captured arena and copy each span in, host-unrolled. Memoised: the first call
    /// traces the alloc + copies and caches the result; later calls return the cache (no re-copy). Host-side
    /// guard -- all trace-time control flow.
    void materialize() const;

    std::vector<Span> spans;
    nautilus::val<Arena*> arena;
    mutable bool materialized{false};
    mutable std::optional<VariableSizedData> materializedData;
};
}
