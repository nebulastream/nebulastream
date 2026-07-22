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

#include <Nautilus/DataTypes/LazyValueRepresentation.hpp>

#include <cstdint>
#include <memory>
#include <ostream>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <LazyValueRepresentations/VARSIZEDLazyValueRepresentation.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <RawValueParser.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
[[nodiscard]] nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const LazyValueRepresentation& lazyValue)
{
    const auto content = lazyValue.getContent();
    oss << "Size(" << lazyValue.size << "): ";
    for (nautilus::val<uint32_t> i = 0; i < lazyValue.size; ++i)
    {
        const nautilus::val<int> byte = readValueFromMemRef<int8_t>((content + i)) & nautilus::val<int>(0xff);
        oss << nautilus::hex;
        oss.operator<<(byte);
        oss << " ";
    }
    return oss;
}

nautilus::val<int8_t*> LazyValueRepresentation::getContent() const
{
    /// Single span (every scalar / passthrough): hand back the view pointer, no copy. `spans.size() == 1` is a
    /// host-time test, so this compiles to just the pointer with no runtime branch.
    if (spans.size() == 1)
    {
        return spans.front().ptr;
    }
    /// A multi-span rope (a concat result) owns no contiguous buffer. It can be written out span-by-span (the
    /// output formatter walks getSpans()), but reading it back as one pointer is not supported.
    throw NotImplemented("Cannot read a multi-span (concatenated) string as one contiguous buffer.");
}

std::shared_ptr<LazyValueRepresentation> LazyValueRepresentation::varSizedView(
    const nautilus::val<int8_t*>& reference, const nautilus::val<uint64_t>& size, const bool nullable, const nautilus::val<bool>& isNull)
{
    const DataType varSizedType{DataType::Type::VARSIZED, nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE};
    return std::make_shared<VARSIZEDLazyValueRepresentation>(reference, size, varSizedType, isNull, "");
}

std::shared_ptr<LazyValueRepresentation> LazyValueRepresentation::asVarSized() const
{
    /// The base (a scalar / numeric field) is a single span over its raw text; a VARSIZED view over the same
    /// bytes is that text read as a string -- no parse, no copy. Numeric lazy values are always single-span.
    return varSizedView(spans.front().ptr, size, type.nullable, isNull);
}

VarVal LazyValueRepresentation::parseValue() const
{
    /// A multi-span rope (a concat result) owns no contiguous buffer, so it cannot be parsed back. It is meant
    /// to be written out (the formatter walks getSpans()); reading it back contiguously is not supported.
    if (spans.size() > 1)
    {
        throw NotImplemented("Cannot read a multi-span (concatenated) string back as a value.");
    }
    /// Single span: parse its raw text into the underlying value dictated by the type.
    switch (type.type)
    {
        case DataType::Type::VARSIZED: {
            return {VariableSizedData(spans.front().ptr, size), type.nullable, isNull};
        }
        case DataType::Type::UNDEFINED: {
            throw UnknownDataType("Not supporting reading {} data type from memory.", magic_enum::enum_name(type.type));
        }
        default: {
            return parseLazyIntoVarVal(type.nullable, isNull, spans.front().ptr, size, parserType);
        }
    }
}

/// Macro to call the reverse function of the lazy value for every case of lhs = Varval, rhs = LazyValue
#define DEFINE_CALL_REVERSE_OP(operatorSign, reverseFunc) \
    VarVal operatorSign(const VarVal& other, const std::shared_ptr<LazyValueRepresentation>& rhs) \
    { \
        return rhs->reverseFunc(other); \
    }

/// Default implementation of operations on lazy values: The lazy value is parsed and the function is applied to the underlying varval
#define DEFAULT_OPERATOR_OVERRIDE(opFunction) \
    VarVal LazyValueRepresentation::opFunction(const VarVal& other) const \
    { \
        const VarVal parsedLazyVal = parseValue(); \
        return parsedLazyVal.opFunction(other); \
    }

/// Same as macro above but for the reverse operations
#define DEFAULT_REVERSE_OVERRIDE(reverseFunction, opFunction) \
    VarVal LazyValueRepresentation::reverseFunction(const VarVal& other) const \
    { \
        const VarVal parsedLazyVal = parseValue(); \
        return other.opFunction(parsedLazyVal); \
    }

VarVal LazyValueRepresentation::operator!() const
{
    const VarVal parsedLazyVal = parseValue();
    return parsedLazyVal.operator!();
}

/// Call reverse op macros for all function types
DEFINE_CALL_REVERSE_OP(operator+, reverseAdd);
DEFINE_CALL_REVERSE_OP(operator-, reverseSub);
DEFINE_CALL_REVERSE_OP(operator*, reverseMul);
DEFINE_CALL_REVERSE_OP(operator/, reverseDiv);
DEFINE_CALL_REVERSE_OP(operator%, reverseMod);
DEFINE_CALL_REVERSE_OP(operator==, reverseEQ)
DEFINE_CALL_REVERSE_OP(operator!=, reverseNEQ)
DEFINE_CALL_REVERSE_OP(operator&&, reverseAND)
DEFINE_CALL_REVERSE_OP(operator||, reverseOR)
DEFINE_CALL_REVERSE_OP(operator<, reverseLT)
DEFINE_CALL_REVERSE_OP(operator>, reverseGT)
DEFINE_CALL_REVERSE_OP(operator<=, reverseLE)
DEFINE_CALL_REVERSE_OP(operator>=, reverseGE)
DEFINE_CALL_REVERSE_OP(operator&, reverseBAND)
DEFINE_CALL_REVERSE_OP(operator|, reverseBOR)
DEFINE_CALL_REVERSE_OP(operator^, reverseXOR)
DEFINE_CALL_REVERSE_OP(operator<<, reverseSHL)
DEFINE_CALL_REVERSE_OP(operator>>, reverseSHR)


/// Create default implementations for all overridden functions
DEFAULT_OPERATOR_OVERRIDE(operator+);
DEFAULT_OPERATOR_OVERRIDE(operator-);
DEFAULT_OPERATOR_OVERRIDE(operator*);
DEFAULT_OPERATOR_OVERRIDE(operator/);
DEFAULT_OPERATOR_OVERRIDE(operator%)
DEFAULT_OPERATOR_OVERRIDE(operator==)
DEFAULT_OPERATOR_OVERRIDE(operator!=)
DEFAULT_OPERATOR_OVERRIDE(operator&&)
DEFAULT_OPERATOR_OVERRIDE(operator||)
DEFAULT_OPERATOR_OVERRIDE(operator<)
DEFAULT_OPERATOR_OVERRIDE(operator>)
DEFAULT_OPERATOR_OVERRIDE(operator<=)
DEFAULT_OPERATOR_OVERRIDE(operator>=)
DEFAULT_OPERATOR_OVERRIDE(operator&)
DEFAULT_OPERATOR_OVERRIDE(operator|)
DEFAULT_OPERATOR_OVERRIDE(operator^)
DEFAULT_OPERATOR_OVERRIDE(operator<<)
DEFAULT_OPERATOR_OVERRIDE(operator>>)

DEFAULT_REVERSE_OVERRIDE(reverseAdd, operator+)
DEFAULT_REVERSE_OVERRIDE(reverseSub, operator-)
DEFAULT_REVERSE_OVERRIDE(reverseMul, operator*)
DEFAULT_REVERSE_OVERRIDE(reverseDiv, operator/)
DEFAULT_REVERSE_OVERRIDE(reverseMod, operator%)
DEFAULT_REVERSE_OVERRIDE(reverseEQ, operator==)
DEFAULT_REVERSE_OVERRIDE(reverseNEQ, operator!=)
DEFAULT_REVERSE_OVERRIDE(reverseAND, operator&&)
DEFAULT_REVERSE_OVERRIDE(reverseOR, operator||)
DEFAULT_REVERSE_OVERRIDE(reverseLT, operator<)
DEFAULT_REVERSE_OVERRIDE(reverseGT, operator>)
DEFAULT_REVERSE_OVERRIDE(reverseLE, operator<=)
DEFAULT_REVERSE_OVERRIDE(reverseGE, operator>=)
DEFAULT_REVERSE_OVERRIDE(reverseBAND, operator&)
DEFAULT_REVERSE_OVERRIDE(reverseBOR, operator|)
DEFAULT_REVERSE_OVERRIDE(reverseXOR, operator^)
DEFAULT_REVERSE_OVERRIDE(reverseSHL, operator<<)
DEFAULT_REVERSE_OVERRIDE(reverseSHR, operator>>)
}
