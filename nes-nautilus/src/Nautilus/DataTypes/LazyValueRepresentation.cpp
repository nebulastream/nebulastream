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
#include <ostream>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/std/ostream.h>
#include <ErrorHandling.hpp>
#include <RawValueParser.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
[[nodiscard]] nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const LazyValueRepresentation& lazyValue)
{
    oss << "Size(" << lazyValue.size << "): ";
    for (nautilus::val<uint32_t> i = 0; i < lazyValue.size; ++i)
    {
        const nautilus::val<int> byte = readValueFromMemRef<int8_t>((lazyValue.ptrToLazyValue + i)) & nautilus::val<int>(0xff);
        oss << nautilus::hex;
        oss.operator<<(byte);
        oss << " ";
    }
    return oss;
}

VarVal LazyValueRepresentation::parseValue() const
{
    /// Switch between possible datatype cases
    switch (type)
    {
        case DataType::Type::UINT8: {
            return parseIntoNautilusRecord<uint8_t>(ptrToLazyValue, size);
        }
        case DataType::Type::UINT16: {
            return parseIntoNautilusRecord<uint16_t>(ptrToLazyValue, size);
        }
        case DataType::Type::UINT32: {
            return parseIntoNautilusRecord<uint32_t>(ptrToLazyValue, size);
        }
        case DataType::Type::UINT64: {
            return parseIntoNautilusRecord<uint64_t>(ptrToLazyValue, size);
        }
        case DataType::Type::INT8: {
            return parseIntoNautilusRecord<int8_t>(ptrToLazyValue, size);
        }
        case DataType::Type::INT16: {
            return parseIntoNautilusRecord<int16_t>(ptrToLazyValue, size);
        }
        case DataType::Type::INT32: {
            return parseIntoNautilusRecord<int32_t>(ptrToLazyValue, size);
        }
        case DataType::Type::INT64: {
            return parseIntoNautilusRecord<int64_t>(ptrToLazyValue, size);
        }
        case DataType::Type::CHAR: {
            return parseIntoNautilusRecord<char>(ptrToLazyValue, size);
        }
        case DataType::Type::BOOLEAN: {
            return parseIntoNautilusRecord<bool>(ptrToLazyValue, size);
        }
        case DataType::Type::FLOAT32: {
            return parseIntoNautilusRecord<float>(ptrToLazyValue, size);
        }
        case DataType::Type::FLOAT64: {
            return parseIntoNautilusRecord<double>(ptrToLazyValue, size);
        }
        case DataType::Type::VARSIZED: {
            return VariableSizedData(ptrToLazyValue, size);
        }
        case DataType::Type::UNDEFINED: {
            throw UnknownDataType("Not supporting reading {} data type from memory.", magic_enum::enum_name(type));
        }
    }
    std::unreachable();
}

/// Macro to call the reverse function of the lazy value for every case of lhs = Varval, rhs = LazyValue
#define DEFINE_CALL_REVERSE_OP(operatorSign, reverseFunc) \
    VarVal operatorSign(const VarVal& other, const LazyValueRepresentation& rhs) \
    { \
        return rhs.reverseFunc(other); \
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
