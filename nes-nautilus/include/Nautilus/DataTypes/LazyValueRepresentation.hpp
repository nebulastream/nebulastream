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
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Util/Strings.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>

namespace NES
{
class RecordBuffer;
class AbstractBufferProvider;
class VarVal;
class LazyValueRepresentation;

/// Implementations for binary functions with the VarVal as lhs.
/// Will call an invoke of the corresponding reverse<opname> method of the LazyValueRepresentation
VarVal operator+(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator-(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator*(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator/(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator%(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator==(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator!=(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator&&(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator||(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator<(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator>(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator<=(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator>=(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator&(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator|(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator^(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator<<(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);
VarVal operator>>(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs);

/// A lazy value is an ordered list of byte SPANS plus a type that says how to read them. It is the single
/// abstraction behind both "a field's raw text, not yet parsed" and "a rope" (a VARSIZED value assembled from
/// several byte views without copying) -- the two used to be separate classes; they are now the span count:
///   - a single span over the "raw string" in the input buffer = a scalar / passthrough field (the common case);
///   - N spans = a rope, built by concatenation (VARSIZEDLazyValueRepresentation::operator+) out of its operands'
///     spans. The rope machinery lives ENTIRELY inside the varsized value type: string operators (CONCAT, CAST)
///     compose plain VarVal operations (`castToType(VARSIZED)`, `+`) and never see a span or a rope, and the
///     output serializer writes a value via writeEachChunk() without knowing whether it is one piece or many.
/// A value is consumed one of two ways. A WRITER (the output formatter, the cross-pipeline store) has a
/// destination and uses writeEachChunk() to write each run straight into it -- one copy, no intermediate buffer;
/// this is every current use of a rope. A READER that needs the bytes contiguous (parse them, compare them)
/// calls parseValue()/getContent(); for a single span that is a no-copy view / a numeric parse, but a multi-span
/// rope owns no contiguous buffer, so reading one back that way is NOT SUPPORTED (it throws) -- a concat result
/// is meant to be written out, not read back. By referring to the raw bytes instead of parsing eagerly we skip
/// the parse AND the reverse-parse in the OutputFormatter -- the whole win for stateless source-to-sink queries.
/// Type-specific subclasses (INT/UINT/FLOAT/VARSIZED) override select operators (`==`, `+`, ...) to work on the
/// raw text directly and avoid the parse. The span list is HOST-TIME (trace-time) scaffolding -- see Span below.
class LazyValueRepresentation
{
public:
    /// A byte span: a (pointer, length) view into some live buffer. A lazy value is an ordered list of these
    /// spans (a "rope", LLVM-Twine style); a plain parse-backed lazy value is the 1-span degenerate case. The
    /// span list is built and iterated in host C++ at trace time -- its size is compile-time, each element
    /// carries nautilus SSA handles -- so consumers host-unroll the walk and never emit a runtime loop over spans.
    struct Span
    {
        nautilus::val<int8_t*> ptr;
        nautilus::val<uint64_t> len;
    };

    /// Scalar / passthrough constructor: a single-span lazy value over `reference` (its raw text in the input
    /// buffer). This is the shape every field read starts as; numeric subclasses parse it lazily via parserType.
    explicit LazyValueRepresentation(
        const nautilus::val<int8_t*>& reference,
        const nautilus::val<uint64_t>& size,
        const DataType& type,
        const nautilus::val<bool>& isNull,
        const std::string& parserType)
        : spans{Span{reference, size}}, size(size), type(type), isNull(isNull), parserType(parserType)
    {
    }

    /// Rope constructor: a value made of N spans (assembled inside the varsized value type by operator+ / the
    /// VARSIZED cast). Always VARSIZED. The spans are borrowed views; the rope owns no storage. Reading such a
    /// value back as one contiguous buffer (materialisation) is not supported -- it can only be written out.
    LazyValueRepresentation(
        std::vector<Span> spans, const nautilus::val<uint64_t>& totalSize, const DataType& type, const nautilus::val<bool>& isNull)
        : spans(std::move(spans)), size(totalSize), type(type), isNull(isNull), parserType("")
    {
    }

    LazyValueRepresentation(const LazyValueRepresentation& other) = default;
    virtual ~LazyValueRepresentation() = default;

    LazyValueRepresentation(LazyValueRepresentation&& other) noexcept
        : spans(std::move(other.spans)), size(other.size), type(other.type), isNull(other.isNull), parserType(std::move(other.parserType))
    {
    }

    LazyValueRepresentation& operator=(const LazyValueRepresentation& other) noexcept
    {
        if (this == &other || other.spans.empty())
        {
            return *this;
        }

        spans = other.spans;
        size = other.size;
        isNull = other.isNull;
        parserType = other.parserType;
        return *this;
    }

    /// Returns one contiguous pointer to the value's bytes. A single-span value (every scalar / passthrough)
    /// hands back its span pointer with no copy. A multi-span rope (a concat result) owns no contiguous buffer,
    /// so this THROWS -- such a value can be written out span-by-span (getSpans) but not read back contiguously.
    /// The `spans.size() == 1` test is host-time (compile-time), so scalars pay no runtime branch.
    [[nodiscard]] nautilus::val<int8_t*> getContent() const;

    /// Reinterpret this value's raw bytes as a VARSIZED string -- what CAST(x AS VARSIZED) forwards. A numeric
    /// field hands back a single-span VARSIZED view over its digits with NO parse; an already-VARSIZED value is
    /// returned unchanged by the caller. This is where the "forward, don't parse" happens; see VarVal::castToType.
    [[nodiscard]] virtual std::shared_ptr<LazyValueRepresentation> asVarSized() const;

    /// Wrap a contiguous (ptr, size) as a single-span lazy VARSIZED value -- the "read these bytes as a string"
    /// primitive. Used by asVarSized() and by CAST-to-VARSIZED of an already-materialised value, so a cast result
    /// is ALWAYS a lazy VARSIZED value that composes with `+` (concat) -- even when both concat operands are
    /// materialised (the interpreted path, or CONCAT of two literals).
    [[nodiscard]] static std::shared_ptr<LazyValueRepresentation> varSizedView(
        const nautilus::val<int8_t*>& reference, const nautilus::val<uint64_t>& size, bool nullable, const nautilus::val<bool>& isNull);

    [[nodiscard]] nautilus::val<uint64_t> getSize() const { return size; }

    /// Returns the ordered byte spans that make up this value. Kept for internal use by the value layer; a
    /// serializer should use writeEachChunk() instead, which hides how many pieces there are.
    [[nodiscard]] std::vector<Span> getSpans() const { return spans; }

    /// Feed this value's bytes to `writeChunk` one contiguous run at a time -- the primitive a serializer uses
    /// to write a string out without knowing whether it is one piece or many. `writeChunk(ptr, len, isFirst,
    /// isLast)` is called once for a passthrough/materialised value and once per segment for a concat rope; the
    /// caller writes each run straight to its destination (one copy, no materialisation) and the fragmentation
    /// stays hidden. The run count is compile-time, so the walk is host-unrolled (a static_val counter keeps the
    /// tracer from mistaking the repeated write for a runtime loop).
    template <typename WriteChunk>
    void writeEachChunk(WriteChunk&& writeChunk) const
    {
        const std::size_t chunkCount = spans.size();
        for (nautilus::static_val<std::size_t> chunkIndex = 0; chunkIndex < chunkCount; ++chunkIndex)
        {
            const std::size_t index = chunkIndex;
            writeChunk(spans[index].ptr, spans[index].len, index == 0, index + 1 == chunkCount);
        }
    }

    /// HOST-ONLY predicate: does this value need span-by-span writing? Kept for internal use by the value layer.
    [[nodiscard]] bool isRope() const { return spans.size() > 1; }

    [[nodiscard]] nautilus::val<bool> getIsNull() const { return isNull; }

    [[nodiscard]] DataType getType() const { return type; }

    /// Method to check if the lazy value has any text behind it.
    /// Usable for some bool function overrides
    [[nodiscard]] nautilus::val<bool> isValid() const { return size > 0 && spans.front().ptr != nullptr; }

    /// Method to check if the lazy values content forms a representation of a boolean true
    /// Usable for some bool function overrides
    [[nodiscard]] nautilus::val<bool> isBooleanTrue() const
    {
        return (getSize() == 1 && nautilus::memcmp(getContent(), nautilus::val<const char*>("1"), 1) == 0)
            || (size == 4
                && (nautilus::memcmp(getContent(), nautilus::val<const char*>("true"), 4) == 0
                    || nautilus::memcmp(getContent(), nautilus::val<const char*>("TRUE"), 4) == 0
                    || nautilus::memcmp(getContent(), nautilus::val<const char*>("True"), 4) == 0));
    }

    LazyValueRepresentation& operator=(LazyValueRepresentation&& other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }

        spans = std::move(other.spans);
        size = other.size;
        isNull = other.isNull;
        parserType = std::move(other.parserType);
        return *this;
    }

    /// Converts the lazy value into a VarVal of the underlying value, as dictated by the type member. A
    /// single-span value parses its raw text (numeric) or wraps it as VariableSizedData (varsized). A multi-span
    /// rope owns no contiguous buffer, so reading it back this way is not supported (it THROWS) -- a concat
    /// result is meant to be written out, not parsed back.
    [[nodiscard]] virtual VarVal parseValue() const;

    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const LazyValueRepresentation& lazyValue);

    /// Overridable logical function implementations. Per default, a lazy value must always be parsed so the function can be executed.
    [[nodiscard]] virtual VarVal operator+(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseAdd(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator-(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseSub(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator*(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseMul(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator/(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseDiv(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator%(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseMod(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator==(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseEQ(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator!=(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseNEQ(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator&&(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseAND(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator||(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseOR(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator<(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseLT(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator>(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseGT(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator<=(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseLE(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator>=(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseGE(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator&(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseBAND(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator|(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseBOR(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator^(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseXOR(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator<<(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseSHL(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator>>(const VarVal& other) const;
    [[nodiscard]] virtual VarVal reverseSHR(const VarVal& other) const;
    [[nodiscard]] virtual VarVal operator!() const;

protected:
    /// The ordered byte spans that make up this value (always >= 1). A single span is a scalar / passthrough
    /// value over its raw text in the input buffer; N spans is a rope (a concat result). Host-time scaffolding:
    /// the vector lives during tracing only, its size is compile-time, each Span carries SSA handles. The spans
    /// are borrowed views (into the input buffer / const globals) -- the value owns no contiguous storage.
    std::vector<Span> spans;
    /// Sigma of the span lengths (the logical string length); a member so getSize() is a cheap read, no walk.
    nautilus::val<uint64_t> size;
    DataType type;
    /// Usually, only a varval should be able to include this information. However, as sometimes, the lazyvalue itself casts itself into a different varval, we need to make an exception.
    nautilus::val<bool> isNull;
    /// The input parser that has to be created if the lazy value is eventually parsed (single-span scalars only).
    std::string parserType;
};
}
