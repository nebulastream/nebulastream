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

#include <LazyValueRepresentations/UINTLazyValueRepresentation.hpp>

#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <std/cstring.h>
#include <LazyValueRepresentationRegistry.hpp>
#include <inline.hpp>
#include <select.hpp>
#include <val_bool.hpp>

namespace NES
{

template <typename T>
NAUTILUS_INLINE static bool constantEq(const int8_t* lazyPtr, const uint64_t lazySize, const T rhs)
{
    const std::string_view lhsString{reinterpret_cast<const char*>(lazyPtr), lazySize};
    /// Will be skipped by compiler by preparing the ascii chars of the constant in memory
    const std::string rhsString = std::to_string(rhs);
    return lhsString == rhsString;
}

template <typename T>
NAUTILUS_INLINE static bool constantLt(const int8_t* lazyPtr, const uint64_t lazySize, const T rhs)
{
    const std::string_view lhsString{reinterpret_cast<const char*>(lazyPtr), lazySize};
    /// UINTs can't be negative
    const bool rhsNegative = rhs < 0;

    bool result = false;
    if (rhsNegative)
    {
        result = false;
    }
    else
    {
        /// Will be skipped by compiler by preparing the ascii chars of the constant in memory
        const std::string rhsString = std::to_string(rhs);
        result = lazySize != rhsString.length() ? rhsNegative ^ (lazySize < rhsString.length()) : lhsString < rhsString;
    }
    return result;
}

nautilus::val<bool> UINTLazyValueRepresentation::eqImpl(const nautilus::val<bool>& rhs) const
{
    nautilus::val<bool> result{false};
    if (isValid())
    {
        /// Check if the value is "0". This assumes that our integers are not padded unnecessary
        result = (nautilus::memcmp(getContent(), nautilus::val<const char*>("0"), 1) == 0) != rhs;
    }
    else
    {
        result = !rhs;
    }
    return result;
}

nautilus::val<bool> UINTLazyValueRepresentation::eqImpl(const VariableSizedData& rhs) const
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

nautilus::val<bool> UINTLazyValueRepresentation::eqImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const
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
        default: {
            /// All unsigned integer types land here
            /// It suffices to compare the contents byte per byte
            result = nautilus::select(
                size != rhs->getSize(), nautilus::val<bool>{false}, (nautilus::memcmp(lhsContent, rhsContent, size) == 0));
        }
    }
    return result;
}

#define CONSTANT_EQ_OVERRIDE(ctype) \
    nautilus::val<bool> UINTLazyValueRepresentation::eqImpl(const nautilus::val<ctype>& rhs) const \
    { \
        return nautilus::invoke(constantEq<ctype>, ptrToLazyValue, size, rhs); \
    }
CONSTANT_EQ_OVERRIDE(uint8_t);
CONSTANT_EQ_OVERRIDE(uint16_t);
CONSTANT_EQ_OVERRIDE(uint32_t);
CONSTANT_EQ_OVERRIDE(uint64_t);

nautilus::val<bool> UINTLazyValueRepresentation::ltImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const
{
    nautilus::val<bool> result{false};
    const auto lhsContent = getContent();
    const auto rhsContent = rhs->getContent();
    result = nautilus::select(size == rhs->getSize(), nautilus::memcmp(lhsContent, rhsContent, size) < 0, size < rhs->getSize());
    return result;
}

#define CONSTANT_LT_OVERRIDE(ctype) \
    nautilus::val<bool> UINTLazyValueRepresentation::ltImpl(const nautilus::val<ctype>& rhs) const \
    { \
        return nautilus::invoke(constantLt<ctype>, ptrToLazyValue, size, rhs); \
    }

CONSTANT_LT_OVERRIDE(uint8_t);
CONSTANT_LT_OVERRIDE(uint16_t);
CONSTANT_LT_OVERRIDE(uint32_t);
CONSTANT_LT_OVERRIDE(uint64_t);

/// All other comparisons can be performed by using the existing implementations

VarVal UINTLazyValueRepresentation::operator==(const VarVal& other) const
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
                if (val->getType().type != DataType::Type::BOOLEAN && (!val->getType().isInteger() || val->getType().isSignedInteger()))
                {
                    return LazyValueRepresentation::operator==({val,rightIsNullable,rightIsNull});
                }
            }
            if constexpr (requires {
                              static_cast<nautilus::val<bool> (UINTLazyValueRepresentation::*)(const T&) const>(
                                  &UINTLazyValueRepresentation::eqImpl);
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
            return LazyValueRepresentation::operator==({val,rightIsNullable,rightIsNull});
        });
}

VarVal UINTLazyValueRepresentation::reverseEQ(const VarVal& other) const
{
    return operator==(other);
}

VarVal UINTLazyValueRepresentation::operator!=(const VarVal& other) const
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
                if (val->getType().type != DataType::Type::BOOLEAN && (!val->getType().isInteger() || val->getType().isSignedInteger()))
                {
                    return LazyValueRepresentation::operator!=({val,rightIsNullable,rightIsNull});
                }
            }

            if constexpr (requires {
                              static_cast<nautilus::val<bool> (UINTLazyValueRepresentation::*)(const T&) const>(
                                  &UINTLazyValueRepresentation::eqImpl);
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
            return LazyValueRepresentation::operator!=({val,rightIsNullable,rightIsNull});
        });
}

VarVal UINTLazyValueRepresentation::reverseNEQ(const VarVal& other) const
{
    return operator!=(other);
}

VarVal UINTLazyValueRepresentation::operator<(const VarVal& other) const
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
                if (!val->getType().isInteger() || val->getType().isSignedInteger())
                {
                    return LazyValueRepresentation::operator<({val,rightIsNullable,rightIsNull});
                }
            }
            if constexpr (requires {
                              static_cast<nautilus::val<bool> (UINTLazyValueRepresentation::*)(const T&) const>(
                                  &UINTLazyValueRepresentation::ltImpl);
                          })
            {
                if (leftIsNullable || rightIsNullable)
                {
                    const auto result = nautilus::select(leftIsNull || rightIsNull, nautilus::val<bool>(false), this->ltImpl(val));
                    return VarVal{result, true, leftIsNull || rightIsNull};
                }
                const auto result = this->ltImpl(val);
                return VarVal{result, false, false};
            }
            return LazyValueRepresentation::operator<({val,rightIsNullable,rightIsNull});
        });
}

VarVal UINTLazyValueRepresentation::reverseLT(const VarVal& other) const
{
    return operator>(other);
}

VarVal UINTLazyValueRepresentation::operator<=(const VarVal& other) const
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
                if (!val->getType().isInteger() || val->getType().isSignedInteger())
                {
                    return LazyValueRepresentation::operator<=({val,rightIsNullable,rightIsNull});
                }
            }

            if constexpr (requires {
                              {
                                  static_cast<nautilus::val<bool> (UINTLazyValueRepresentation::*)(const T&) const>(
                                      &UINTLazyValueRepresentation::ltImpl)
                              };
                              {
                                  static_cast<nautilus::val<bool> (UINTLazyValueRepresentation::*)(const T&) const>(
                                      &UINTLazyValueRepresentation::eqImpl)
                              };
                          })
            {
                if (leftIsNullable || rightIsNullable)
                {
                    const auto result
                        = nautilus::select(leftIsNull || rightIsNull, nautilus::val<bool>(false), this->eqImpl(val) || this->ltImpl(val));
                    return VarVal{result, true, leftIsNull || rightIsNull};
                }
                const auto result = this->eqImpl(val) || this->ltImpl(val);
                return VarVal{result, false, false};
            }
            return LazyValueRepresentation::operator<=({val,rightIsNullable,rightIsNull});
        });
}

VarVal UINTLazyValueRepresentation::reverseLE(const VarVal& other) const
{
    return operator>=(other);
}

VarVal UINTLazyValueRepresentation::operator>(const VarVal& other) const
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
                if (!val->getType().isInteger() || val->getType().isSignedInteger())
                {
                    return LazyValueRepresentation::operator>({val,rightIsNullable,rightIsNull});
                }
            }

            if constexpr (requires {
                              {
                                  static_cast<nautilus::val<bool> (UINTLazyValueRepresentation::*)(const T&) const>(
                                      &UINTLazyValueRepresentation::ltImpl)
                              };
                              {
                                  static_cast<nautilus::val<bool> (UINTLazyValueRepresentation::*)(const T&) const>(
                                      &UINTLazyValueRepresentation::eqImpl)
                              };
                          })
            {
                if (leftIsNullable || rightIsNullable)
                {
                    const auto result
                        = nautilus::select(leftIsNull || rightIsNull, nautilus::val<bool>(false), !this->eqImpl(val) && !this->ltImpl(val));
                    return VarVal{result, true, leftIsNull || rightIsNull};
                }
                const auto result = !this->eqImpl(val) && !this->ltImpl(val);
                return VarVal{result, false, false};
            }
            return LazyValueRepresentation::operator>({val,rightIsNullable,rightIsNull});
        });
}

VarVal UINTLazyValueRepresentation::reverseGT(const VarVal& other) const
{
    return operator<(other);
}

VarVal UINTLazyValueRepresentation::operator>=(const VarVal& other) const
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
                if (!val->getType().isInteger() || val->getType().isSignedInteger())
                {
                    return LazyValueRepresentation::operator>=({val,rightIsNullable,rightIsNull});
                }
            }

            if constexpr (requires {
                              static_cast<nautilus::val<bool> (UINTLazyValueRepresentation::*)(const T&) const>(
                                  &UINTLazyValueRepresentation::ltImpl);
                          })
            {
                if (leftIsNullable || rightIsNullable)
                {
                    const auto result = nautilus::select(leftIsNull || rightIsNull, nautilus::val<bool>(false), !this->ltImpl(val));
                    return VarVal{result, true, leftIsNull || rightIsNull};
                }
                const auto result = !this->ltImpl(val);
                return VarVal{result, false, false};
            }
            return LazyValueRepresentation::operator>=({val,rightIsNullable,rightIsNull});
        });
}

VarVal UINTLazyValueRepresentation::reverseGE(const VarVal& other) const
{
    return operator<=(other);
}

LazyValueRepresentationRegistryReturnType
LazyValueRepresentationGeneratedRegistrar::RegisterUINT8LazyValueRepresentation(LazyValueRepresentationRegistryArguments args)
{
    return std::make_shared<UINTLazyValueRepresentation>(args.valueAddress, args.size, args.type, args.isNull);
}

LazyValueRepresentationRegistryReturnType
LazyValueRepresentationGeneratedRegistrar::RegisterUINT16LazyValueRepresentation(LazyValueRepresentationRegistryArguments args)
{
    return std::make_shared<UINTLazyValueRepresentation>(args.valueAddress, args.size, args.type, args.isNull);
}

LazyValueRepresentationRegistryReturnType
LazyValueRepresentationGeneratedRegistrar::RegisterUINT32LazyValueRepresentation(LazyValueRepresentationRegistryArguments args)
{
    return std::make_shared<UINTLazyValueRepresentation>(args.valueAddress, args.size, args.type, args.isNull);
}

LazyValueRepresentationRegistryReturnType
LazyValueRepresentationGeneratedRegistrar::RegisterUINT64LazyValueRepresentation(LazyValueRepresentationRegistryArguments args)
{
    return std::make_shared<UINTLazyValueRepresentation>(args.valueAddress, args.size, args.type, args.isNull);
}
}
