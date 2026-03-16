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

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <LazyValueRepresentations/FLOATLazyValueRepresentation.hpp>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/LazyValueRepresentation.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <std/cstring.h>
#include <LazyValueRepresentationRegistry.hpp>
#include <function.hpp>
#include <inline.hpp>
#include <select.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// For lazy floats, comparing to constant floats is not always possible, due to rounding.
/// We therefore only allow float - int comparisons here
template <typename T>
NAUTILUS_INLINE static bool constantEq(const int8_t* lazyPtr, const uint64_t lazySize, const T rhs)
{
    const std::string_view lhsString{reinterpret_cast<const char*>(lazyPtr), lazySize};
    /// Will be skipped by compiler by preparing the ascii chars of the constant in memory
    const std::string rhsString = std::to_string(rhs);
    /// Take the substring of lhsString, that does not include trailing zeros
    size_t trueSize = lazySize;
    for (size_t i = lazySize; i > 0; --i)
    {
        if (lhsString[i - 1] == '.')
        {
            --trueSize;
            break;
        }
        if (lhsString[i - 1] != '0')
        {
            break;
        }
        --trueSize;
    }
    const std::string_view trueLhsView{lhsString.data(), trueSize};
    return trueLhsView == rhsString;
}

/// Compares a lazy float lhs to a lazy integer rhs
NAUTILUS_INLINE static bool lazyEqualFloatInt(const int8_t* lhsPtr, const uint64_t lhsSize, const int8_t* rhsPtr, const uint64_t rhsSize)
{
    /// RHS is an int. We create string views for both and cut lhs as much as possible
    const std::string_view lhsString{reinterpret_cast<const char*>(lhsPtr), lhsSize};
    const std::string_view rhsString{reinterpret_cast<const char*>(rhsPtr), rhsSize};
    size_t trueSize = lhsSize;
    for (size_t i = lhsSize; i > 0; --i)
    {
        if (lhsString[i - 1] == '.')
        {
            --trueSize;
            break;
        }
        if (lhsString[i - 1] != '0')
        {
            break;
        }
        --trueSize;
    }
    const std::string_view trueLhsView{lhsString.data(), trueSize};
    return trueLhsView == rhsString;
}

nautilus::val<bool> FLOATLazyValueRepresentation::eqImpl(const nautilus::val<bool>& rhs) const
{
    nautilus::val<bool> result{false};
    result = nautilus::select(isValid(), (nautilus::memcmp(getContent(), nautilus::val<const char*>("0.0"), 3) == 0) != rhs, !rhs);
    return result;
}

nautilus::val<bool> FLOATLazyValueRepresentation::eqImpl(const VariableSizedData& rhs) const
{
    nautilus::val<bool> result{false};
    /// Lazy Values make String-Numeric comparisons very trivial
    if (size != rhs.getSize())
    {
        result = nautilus::val<bool>(false);
    }
    else
    {
        const auto lazyData = getContent();
        const auto rhsVarSizedData = rhs.getContent();
        result = (nautilus::memcmp(lazyData, rhsVarSizedData, size) == 0);
    }
    return result;
}

nautilus::val<bool> FLOATLazyValueRepresentation::eqImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const
{
    nautilus::val<bool> result{false};
    const auto lhsContent = getContent();
    auto rhsContent = rhs->getContent();
    switch (rhs->getType().type)
    {
        /// Get the bool val out of rhs and perform eqImpl
        case DataType::Type::BOOLEAN: {
            result = eqImpl(rhs->isBooleanTrue());
            break;
        }
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64: {
            /// It suffices to compare the contents byte per byte
            result = nautilus::select(
                size != rhs->getSize(), nautilus::val<bool>(false), (nautilus::memcmp(lhsContent, rhsContent, size) == 0));
            break;
        }
        default: {
            result = nautilus::invoke(lazyEqualFloatInt, lhsContent, size, rhsContent, rhs->getSize());
        }
    }
    return result;
}

#define CONSTANT_EQ_OVERRIDE(ctype) \
    nautilus::val<bool> FLOATLazyValueRepresentation::eqImpl(const nautilus::val<ctype>& rhs) const \
    { \
        return nautilus::invoke(constantEq<ctype>, ptrToLazyValue, size, rhs); \
    }
CONSTANT_EQ_OVERRIDE(int8_t);
CONSTANT_EQ_OVERRIDE(int16_t);
CONSTANT_EQ_OVERRIDE(int32_t);
CONSTANT_EQ_OVERRIDE(int64_t);
CONSTANT_EQ_OVERRIDE(uint8_t);
CONSTANT_EQ_OVERRIDE(uint16_t);
CONSTANT_EQ_OVERRIDE(uint32_t);
CONSTANT_EQ_OVERRIDE(uint64_t);

VarVal FLOATLazyValueRepresentation::operator==(const VarVal& other) const
{
    return other.customVisit(
        [this,
         leftIsNullable = this->type.nullable,
         rightIsNullable = other.isNullable(),
         leftIsNull = this->isNull,
         rightIsNull = other.isNull()]<typename T>(const T& val) -> VarVal
        {
            if constexpr (std::is_same_v<T, std::shared_ptr<LazyValueRepresentation>>)
            {
                if (val->getType().type != DataType::Type::BOOLEAN && !val->getType().isNumeric())
                {
                    return LazyValueRepresentation::operator==({val, rightIsNullable, rightIsNull});
                }
            }
            if constexpr (requires {
                              static_cast<nautilus::val<bool> (FLOATLazyValueRepresentation::*)(const T&) const>(
                                  &FLOATLazyValueRepresentation::eqImpl);
                          })
            {
                if (leftIsNullable || rightIsNullable)
                {
                    const auto result = nautilus::select(leftIsNull || rightIsNull, nautilus::val<bool>(false), this->eqImpl(val));
                    return VarVal{result, true, leftIsNull || rightIsNull};
                }
                const auto result = this->eqImpl(val);
                return VarVal{result, false, false};
            }
            return LazyValueRepresentation::operator==({val, rightIsNullable, rightIsNull});
        });
}

VarVal FLOATLazyValueRepresentation::reverseEQ(const VarVal& other) const
{
    return operator==(other);
}

VarVal FLOATLazyValueRepresentation::operator!=(const VarVal& other) const
{
    return other.customVisit(
        [this,
         leftIsNullable = this->type.nullable,
         rightIsNullable = other.isNullable(),
         leftIsNull = this->isNull,
         rightIsNull = other.isNull()]<typename T>(const T& val) -> VarVal
        {
            if constexpr (std::is_same_v<T, std::shared_ptr<LazyValueRepresentation>>)
            {
                if (val->getType().type != DataType::Type::BOOLEAN && !val->getType().isNumeric())
                {
                    return LazyValueRepresentation::operator!=({val, rightIsNullable, rightIsNull});
                }
            }

            if constexpr (requires {
                              static_cast<nautilus::val<bool> (FLOATLazyValueRepresentation::*)(const T&) const>(
                                  &FLOATLazyValueRepresentation::eqImpl);
                          })
            {
                if (leftIsNullable || rightIsNullable)
                {
                    const auto result = nautilus::select(leftIsNull || rightIsNull, nautilus::val<bool>(false), !this->eqImpl(val));
                    return VarVal{result, true, leftIsNull || rightIsNull};
                }
                const auto result = !this->eqImpl(val);
                return VarVal{result, false, false};
            }
            return LazyValueRepresentation::operator!=({val, rightIsNullable, rightIsNull});
        });
}

VarVal FLOATLazyValueRepresentation::reverseNEQ(const VarVal& other) const
{
    return operator!=(other);
}

LazyValueRepresentationRegistryReturnType
LazyValueRepresentationGeneratedRegistrar::RegisterFLOAT32LazyValueRepresentation(LazyValueRepresentationRegistryArguments args)
{
    return std::make_shared<FLOATLazyValueRepresentation>(args.valueAddress, args.size, args.type, args.isNull);
}

LazyValueRepresentationRegistryReturnType
LazyValueRepresentationGeneratedRegistrar::RegisterFLOAT64LazyValueRepresentation(LazyValueRepresentationRegistryArguments args)
{
    return std::make_shared<FLOATLazyValueRepresentation>(args.valueAddress, args.size, args.type, args.isNull);
}
}
