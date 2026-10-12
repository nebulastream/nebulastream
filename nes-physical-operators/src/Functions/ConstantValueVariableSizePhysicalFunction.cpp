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

#include <Functions/ConstantValueVariableSizePhysicalFunction.hpp>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <Nautilus/DataTypes/LazyValueRepresentation.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/TraceConstantBytesConfig.hpp>
#include <ExecutionContext.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
/// Host-time check: are the literal bytes a valid JSON string body verbatim? True iff none of them is a `"`
/// (0x22), a `\` (0x5C), or a control byte (< 0x20) -- exactly the bytes the JSON output escape scan looks for
/// (see jsonSpanNeedsEscape in JSONOutputFormatter). A clean literal can be forwarded through the inlined span
/// writer with no per-row escape scan; a dirty one keeps the escaping path.
bool computeLiteralIsClean(const std::vector<int8_t>& data)
{
    return std::ranges::none_of(
        data,
        [](const int8_t byte)
        {
            const auto character = static_cast<unsigned char>(byte);
            return character == '"' || character == '\\' || character < 0x20;
        });
}
}

ConstantValueVariableSizePhysicalFunction::ConstantValueVariableSizePhysicalFunction(const int8_t* value, const size_t size)
    : data(size), literalIsClean(false)
{
    /// We copy the value into the data vector owned by the function
    /// since the value might be destroyed during the lifetime of the function.
    /// In the constructor, we have allocated the memory for the value via the std::vector ctor.
    std::memcpy(data.data(), value, size);
    /// Compute the clean property once, host-side, from the now-owned bytes (see literalIsClean).
    literalIsClean = computeLiteralIsClean(data);
}

VarVal ConstantValueVariableSizePhysicalFunction::execute(const Record&, ArenaRef&) const
{
    /// Return the literal as a single-span LAZY VARSIZED value (not a materialised VariableSizedData). This is
    /// the same representation a passthrough field / a CONCAT rope carries, so the value reaches the JSON output
    /// side through writeEachChunk and -- when the bytes are clean (CLEAN|JSON_ESCAPED asserted below) -- takes
    /// the inlined span writer that constant-forwards + raw-copies, instead of the opaque escaping writer the
    /// old VariableSizedData return always hit. Lazy VARSIZED stays transparent to other consumers: parseValue()
    /// yields a VariableSizedData, castToType(VARSIZED) forwards it unchanged (so CONCAT flattens), and the
    /// VARSIZED value type's operators handle string comparison directly.
    ///
    /// Embed the literal's bytes as a private constant global in the JIT module instead of an opaque host pointer:
    /// downstream operations then have a compile-time-visible source and can fold. In interpreted mode
    /// (trace_constant_bytes=false) this degrades to the host pointer into `data` (owned by this function, which
    /// outlives execution) -- the pre-4dddb90d2b shape, where the backend must reload the bytes every row.
    nautilus::val<int8_t*> reference = [&]
    {
        if (traceConstantBytesEnabled())
        {
            return static_cast<nautilus::val<int8_t*>>(nautilus::embedConstantBytes(data.data(), data.size()));
        }
        /// NOLINTBEGIN(cppcoreguidelines-pro-type-const-cast) - the span pointer is read-only; data is never written through
        return nautilus::val<int8_t*>{const_cast<int8_t*>(data.data())};
        /// NOLINTEND(cppcoreguidelines-pro-type-const-cast)
    }();

    auto lazyValue = LazyValueRepresentation::varSizedView(
        reference, nautilus::val<uint64_t>{data.size()}, /*nullable*/ false, nautilus::val<bool>{false});
    /// Only a clean literal may be forwarded verbatim into a JSON string field. CLEAN implies (and is co-set with)
    /// the weaker JSON_ESCAPED, matching the input formatters' assertion for clean CSV text. A dirty literal keeps
    /// the default NONE, so the JSON output side scans + escapes it (correctly) instead of forwarding it raw.
    if (literalIsClean)
    {
        lazyValue->setCharacteristics(Characteristic::CLEAN | Characteristic::JSON_ESCAPED);
    }
    return {std::move(lazyValue), /*nullable*/ false, nautilus::val<bool>{false}};
}

}
