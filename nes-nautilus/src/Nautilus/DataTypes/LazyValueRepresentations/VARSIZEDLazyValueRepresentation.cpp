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

#include <LazyValueRepresentations/VARSIZEDLazyValueRepresentation.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <nautilus/std/cstring.h>
#include <LazyValueRepresentationRegistry.hpp>
#include <function.hpp>
#include <select.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>

namespace NES
{

namespace
{
/// Collect the ordered byte-spans that make up a VarVal's string form. A lazy value forwards its spans with NO
/// parse (an int/float field -- already cast to a VARSIZED view -- hands back its raw digits; a nested rope
/// forwards its whole span list, so concatenation FLATTENS); a materialised VariableSizedData / VARSIZED literal
/// is one span. This is the rope's private business -- no operator or serializer ever sees it.
std::vector<LazyValueRepresentation::Span> spansOf(const VarVal& value)
{
    if (value.isLazyValue())
    {
        return value.getRawValueAs<std::shared_ptr<LazyValueRepresentation>>()->getSpans();
    }
    const auto variableSizedData = value.getRawValueAs<VariableSizedData>();
    return {LazyValueRepresentation::Span{variableSizedData.getContent(), variableSizedData.getSize()}};
}

/// Wrap an ordered span list as a VARSIZED rope. Total length is summed host-side with a static_val counter (a
/// plain per-span `+=` at one source line would look like a loop back-edge to the nautilus tracer). A null
/// result reports logical size 0 (the formatter short-circuits on isNull before walking the spans). The rope is
/// a VARSIZEDLazyValueRepresentation so a further concat dispatches back to operator+ and flattens.
VarVal makeRope(std::vector<LazyValueRepresentation::Span> spans, const bool nullable, const nautilus::val<bool>& isNull)
{
    nautilus::val<uint64_t> totalSize{0};
    for (nautilus::static_val<std::size_t> spanIndex = 0; spanIndex < spans.size(); ++spanIndex)
    {
        const std::size_t index = spanIndex;
        totalSize += spans[index].len;
    }
    const auto ropeSize = nullable ? nautilus::select(isNull, nautilus::val<uint64_t>{0}, totalSize) : totalSize;
    const DataType type{DataType::Type::VARSIZED, nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE};
    auto rope = std::make_shared<VARSIZEDLazyValueRepresentation>(std::move(spans), ropeSize, type, isNull);
    return VarVal{std::static_pointer_cast<LazyValueRepresentation>(rope), nullable, isNull};
}

}

VarVal VARSIZEDLazyValueRepresentation::operator+(const VarVal& other) const
{
    /// `this || other`: this value's spans (which is its whole list -- a nested rope contributes all its
    /// segments, so concatenation flattens) followed by the other operand's.
    std::vector<Span> spans = getSpans();
    const auto otherSpans = spansOf(other);
    spans.insert(spans.end(), otherSpans.begin(), otherSpans.end());
    const bool anyNullable = type.nullable or other.isNullable();
    const auto resultNull = anyNullable ? (isNull or other.isNull()) : nautilus::val<bool>{false};
    return makeRope(std::move(spans), anyNullable, resultNull);
}

VarVal VARSIZEDLazyValueRepresentation::reverseAdd(const VarVal& other) const
{
    /// `other || this`: other (the lhs, e.g. a VARSIZED literal) comes first, then this value's spans.
    std::vector<Span> spans = spansOf(other);
    const auto thisSpans = getSpans();
    spans.insert(spans.end(), thisSpans.begin(), thisSpans.end());
    const bool anyNullable = other.isNullable() or type.nullable;
    const auto resultNull = anyNullable ? (other.isNull() or isNull) : nautilus::val<bool>{false};
    return makeRope(std::move(spans), anyNullable, resultNull);
}

nautilus::val<bool> VARSIZEDLazyValueRepresentation::eqImpl(const nautilus::val<bool>& rhs) const
{
    return isValid() == rhs;
}

nautilus::val<bool> VARSIZEDLazyValueRepresentation::bytesEqualTo(const nautilus::val<int8_t*>& rhsContent) const
{
    /// Single span: one contiguous compare (the passthrough fast path, unchanged). A concat rope walks its
    /// segments against the contiguous rhs -- comparing in place, no materialisation and no arena.
    if (spans.size() == 1)
    {
        return emitBytesEqual(getContent(), rhsContent, size);
    }
    nautilus::val<bool> equal{true};
    nautilus::val<uint64_t> offset{0};
    for (nautilus::static_val<std::size_t> spanIndex = 0; spanIndex < spans.size(); ++spanIndex)
    {
        const std::size_t index = spanIndex;
        equal = equal and (nautilus::memcmp(spans[index].ptr, rhsContent + offset, spans[index].len) == 0);
        offset += spans[index].len;
    }
    return equal;
}

nautilus::val<bool> VARSIZEDLazyValueRepresentation::eqImpl(const VariableSizedData& rhs) const
{
    if (size != rhs.getSize())
    {
        return nautilus::val<bool>(false);
    }
    return bytesEqualTo(rhs.getContent());
}

nautilus::val<bool> VARSIZEDLazyValueRepresentation::eqImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const
{
    nautilus::val<bool> result{false};
    if (rhs->getType().type == DataType::Type::BOOLEAN)
    {
        result = rhs->isBooleanTrue() == isValid();
    }
    else if (size != rhs->getSize())
    {
        result = nautilus::val<bool>(false);
    }
    else
    {
        /// rhs->getContent() is contiguous for a passthrough / literal; a concat rope on the rhs is the one
        /// unsupported shape (rope == rope), which is not exercised. bytesEqualTo walks our side if we are a rope.
        result = bytesEqualTo(rhs->getContent());
    }
    return result;
}

nautilus::val<bool> VARSIZEDLazyValueRepresentation::neqImpl(const nautilus::val<bool>& rhs) const
{
    return isValid() != rhs;
}

nautilus::val<bool> VARSIZEDLazyValueRepresentation::neqImpl(const VariableSizedData& rhs) const
{
    return !eqImpl(rhs);
}

nautilus::val<bool> VARSIZEDLazyValueRepresentation::neqImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const
{
    return !eqImpl(rhs);
}

VarVal VARSIZEDLazyValueRepresentation::operator!() const
{
    if (type.nullable)
    {
        const auto result = nautilus::select(isNull, nautilus::val<bool>{false}, !isValid());
        return VarVal(result, true, isNull);
    }
    return VarVal{!isValid(), false, false};
}

VarVal VARSIZEDLazyValueRepresentation::operator==(const VarVal& other) const
{
    return other.customVisit(
        [this,
         leftIsNullable = this->type.nullable,
         rightIsNullable = other.isNullable(),
         leftIsNull = this->isNull,
         rightIsNull = other.isNull()]<typename T>(const T& val) -> VarVal
        {
            if constexpr (requires {
                              static_cast<nautilus::val<bool> (VARSIZEDLazyValueRepresentation::*)(const T&) const>(
                                  &VARSIZEDLazyValueRepresentation::eqImpl);
                          })
            {
                if (leftIsNullable || rightIsNullable)
                {
                    const auto result = nautilus::select(leftIsNull || rightIsNull, nautilus::val<bool>{false}, this->eqImpl(val));
                    return VarVal{result, true, leftIsNull || rightIsNull};
                }
                const auto result = this->eqImpl(val);
                return VarVal{result, false, false};
            }
            return LazyValueRepresentation::operator==({val, rightIsNullable, rightIsNull});
        });
}

VarVal VARSIZEDLazyValueRepresentation::reverseEQ(const VarVal& other) const
{
    return operator==(other);
}

VarVal VARSIZEDLazyValueRepresentation::operator!=(const VarVal& other) const
{
    return other.customVisit(
        [this,
         leftIsNullable = this->type.nullable,
         rightIsNullable = other.isNullable(),
         leftIsNull = this->isNull,
         rightIsNull = other.isNull()]<typename T>(const T& val) -> VarVal
        {
            if constexpr (requires {
                              static_cast<nautilus::val<bool> (VARSIZEDLazyValueRepresentation::*)(const T&) const>(
                                  &VARSIZEDLazyValueRepresentation::neqImpl);
                          })
            {
                if (leftIsNullable || rightIsNullable)
                {
                    const auto result = nautilus::select(leftIsNull || rightIsNull, nautilus::val<bool>{false}, this->neqImpl(val));
                    return VarVal{result, true, leftIsNull || rightIsNull};
                }
                const auto result = this->neqImpl(val);
                return VarVal{result, false, false};
            }
            return LazyValueRepresentation::operator!=({val, rightIsNullable, rightIsNull});
        });
}

VarVal VARSIZEDLazyValueRepresentation::reverseNEQ(const VarVal& other) const
{
    return operator!=(other);
}

LazyValueRepresentationRegistryReturnType
LazyValueRepresentationGeneratedRegistrar::RegisterVARSIZEDLazyValueRepresentation(LazyValueRepresentationRegistryArguments args)
{
    return std::make_shared<VARSIZEDLazyValueRepresentation>(args.valueAddress, args.size, args.type, args.isNull, args.parserType);
}
}
