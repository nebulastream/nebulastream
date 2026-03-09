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

#include <LazyValueRepresentations/INTLazyValueRepresentation.hpp>

#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <LazyValueRepresentationRegistry.hpp>
#include <val_bool.hpp>

namespace NES
{
VarVal INTLazyValueRepresentation::eqImpl(const nautilus::val<bool>& rhs) const
{
    VarVal result = nautilus::val<bool>{false};
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

VarVal INTLazyValueRepresentation::eqImpl(const VariableSizedData& rhs) const
{
    VarVal result = nautilus::val<bool>{false};
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

VarVal INTLazyValueRepresentation::eqImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const
{
    VarVal result = nautilus::val<bool>{false};
    const auto lhsContent = getContent();
    auto rhsContent = rhs->getContent();
    switch (rhs->getType())
    {
        /// Get the bool val out of rhs and perform eqImpl
        case DataType::Type::BOOLEAN: {
            result = eqImpl(rhs->isValid());
            break;
        }
        case DataType::Type::CHAR: {
            /// Not allowed, fall back to default implementation
            result = LazyValueRepresentation::operator==(rhs);
            break;
        }
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64: {
            /// Check if rhs does not have any decimal places
            /// If yes, we can perform value comparisons
            /// Otherwise, we parse to make sure
            if (nautilus::memchr(nautilus::val<const void*>(rhsContent), nautilus::val<int>('.'), rhs->getSize()) == nullptr)
            {
                if (size != rhs->getSize())
                {
                    result = nautilus::val<bool>(false);
                }
                else
                {
                    result = (nautilus::memcmp(lhsContent, rhsContent, size) == 0);
                }
            }
            else
            {
                result = LazyValueRepresentation::operator==(rhs);
            }
            break;
        }
        default: {
            /// All integer types land here
            /// It suffices to compare the contents byte per byte
            if (size != rhs->getSize())
            {
                result = nautilus::val<bool>(false);
            }
            else
            {
                result = (nautilus::memcmp(lhsContent, rhsContent, size) == 0);
            }
        }
    }
    return result;
}

VarVal INTLazyValueRepresentation::neqImpl(const nautilus::val<bool>& rhs) const
{
    return !eqImpl(rhs);
}

VarVal INTLazyValueRepresentation::neqImpl(const VariableSizedData& rhs) const
{
    return !eqImpl(rhs);
}

VarVal INTLazyValueRepresentation::neqImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const
{
    return !eqImpl(rhs);
}

VarVal INTLazyValueRepresentation::ltImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const
{
    VarVal result = nautilus::val<bool>{false};
    const auto lhsContent = getContent();
    auto rhsContent = rhs->getContent();
    switch (rhs->getType())
    {
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64: {
            /// Lazy lt for floats can only be performed, if the floats do not have a decimal point
            /// otherwise, we need to parse
            if (nautilus::memchr(nautilus::val<const void*>(rhsContent), nautilus::val<int>('.'), rhs->getSize()) != nullptr)
            {
                result = LazyValueRepresentation::operator<(rhs);
                break;
            }
        }
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::UINT8:
        case DataType::Type::UINT16: {
            const nautilus::val<bool> lhsNegative = nautilus::memcmp(lhsContent, nautilus::val<const char*>("-"), 1) == 0;
            const nautilus::val<bool> rhsNegative = nautilus::memcmp(rhsContent, nautilus::val<const char*>("-"), 1) == 0;
            if (lhsNegative && !rhsNegative)
            {
                result = nautilus::val<bool>(true);
            }
            else if (!lhsNegative && rhsNegative)
            {
                result = nautilus::val<bool>(false);
            }
            else if (size == rhs->getSize())
            {
                /// Use memcmp to solve < for equal sized lazy vals
                auto res = nautilus::memcmp(lhsContent, rhsContent, size);
                if (lhsNegative && rhsNegative)
                {
                    result = (res > 0);
                }
                else
                {
                    result = (res < 0);
                }
            }
            else
            {
                /// If the lhs has less bytes than rhs, it must be of lesser numeric value
                /// assuming that no padding is involved
                if (lhsNegative && rhsNegative)
                {
                    result = size > rhs->getSize();
                } else
                {
                    result = size < rhs->getSize();
                }
            }
            break;
        }
        /// Starting with UINT32, we need to be careful, as for some int comparisons, the int will be casted to uint, making it impossible to accuratly infer the result of the comparison from the textual data
        case DataType::Type::UINT32: {
            if (type == DataType::Type::INT64)
            {
                const nautilus::val<bool> lhsNegative = nautilus::memcmp(lhsContent, nautilus::val<const char*>("-"), 1) == 0;
                if (lhsNegative)
                {
                    result = nautilus::val<bool>(true);
                }
                else if (size == rhs->getSize())
                {
                    /// Use memcmp to solve < for equal sized lazy vals
                    auto res = nautilus::memcmp(lhsContent, rhsContent, size);
                    result = (res < 0);
                } else
                {
                    /// If the lhs has less bytes than rhs, it must be of lesser numeric value
                    /// assuming that no padding is involved
                    result = size < rhs->getSize();
                }
            }
            else
            {
                result = LazyValueRepresentation::operator<(rhs);
            }
            break;
        }
        default: {
            /// Parse the value for any other datatype
            /// If lt is not supported, a error will be thrown eventually
            result = LazyValueRepresentation::operator<(rhs);
        }
    }
    return result;
}

/// All other comparisons can be performed by using the existing implementations
VarVal INTLazyValueRepresentation::leImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const
{
    return eqImpl(rhs) || ltImpl(rhs);
}

VarVal INTLazyValueRepresentation::gtImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const
{
    return !leImpl(rhs);
}

VarVal INTLazyValueRepresentation::geImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const
{
    return !ltImpl(rhs);
}

VarVal INTLazyValueRepresentation::operator==(const VarVal& other) const
{
    return other.customVisit(
        [&]<typename T>(const T& val) -> VarVal
        {
            if constexpr (requires {
                              static_cast<VarVal (INTLazyValueRepresentation::*)(const T&) const>(&INTLazyValueRepresentation::eqImpl);
                          })
            {
                return this->eqImpl(val);
            }
            return LazyValueRepresentation::operator==(val);
        });
}

VarVal INTLazyValueRepresentation::reverseEQ(const VarVal& other) const
{
    return operator==(other);
}

VarVal INTLazyValueRepresentation::operator!=(const VarVal& other) const
{
    return other.customVisit(
        [&]<typename T>(const T& val) -> VarVal
        {
            if constexpr (requires {
                              static_cast<VarVal (INTLazyValueRepresentation::*)(const T&) const>(&INTLazyValueRepresentation::neqImpl);
                          })
            {
                return this->neqImpl(val);
            }
            return LazyValueRepresentation::operator!=(val);
        });
}

VarVal INTLazyValueRepresentation::reverseNEQ(const VarVal& other) const
{
    return operator!=(other);
}

VarVal INTLazyValueRepresentation::operator<(const VarVal& other) const
{
    return other.customVisit(
        [&]<typename T>(const T& val) -> VarVal
        {
            if constexpr (requires {
                              static_cast<VarVal (INTLazyValueRepresentation::*)(const T&) const>(&INTLazyValueRepresentation::ltImpl);
                          })
            {
                return this->ltImpl(val);
            }
            return LazyValueRepresentation::operator<(val);
        });
}

VarVal INTLazyValueRepresentation::reverseLT(const VarVal& other) const
{
    return operator>(other);
}

VarVal INTLazyValueRepresentation::operator<=(const VarVal& other) const
{
    return other.customVisit(
        [&]<typename T>(const T& val) -> VarVal
        {
            if constexpr (requires {
                              static_cast<VarVal (INTLazyValueRepresentation::*)(const T&) const>(&INTLazyValueRepresentation::leImpl);
                          })
            {
                return this->leImpl(val);
            }
            return LazyValueRepresentation::operator<=(val);
        });
}

VarVal INTLazyValueRepresentation::reverseLE(const VarVal& other) const
{
    return operator>=(other);
}

VarVal INTLazyValueRepresentation::operator>(const VarVal& other) const
{
    return other.customVisit(
        [&]<typename T>(const T& val) -> VarVal
        {
            if constexpr (requires {
                              static_cast<VarVal (INTLazyValueRepresentation::*)(const T&) const>(&INTLazyValueRepresentation::gtImpl);
                          })
            {
                return this->gtImpl(val);
            }
            return LazyValueRepresentation::operator>(val);
        });
}

VarVal INTLazyValueRepresentation::reverseGT(const VarVal& other) const
{
    return operator<(other);
}

VarVal INTLazyValueRepresentation::operator>=(const VarVal& other) const
{
    return other.customVisit(
        [&]<typename T>(const T& val) -> VarVal
        {
            if constexpr (requires {
                              static_cast<VarVal (INTLazyValueRepresentation::*)(const T&) const>(&INTLazyValueRepresentation::geImpl);
                          })
            {
                return this->geImpl(val);
            }
            return LazyValueRepresentation::operator>=(val);
        });
}

VarVal INTLazyValueRepresentation::reverseGE(const VarVal& other) const
{
    return operator<=(other);
}

LazyValueRepresentationRegistryReturnType
LazyValueRepresentationGeneratedRegistrar::RegisterINT8LazyValueRepresentation(LazyValueRepresentationRegistryArguments args)
{
    return std::make_shared<INTLazyValueRepresentation>(args.valueAddress, args.size, args.type);
}

LazyValueRepresentationRegistryReturnType
LazyValueRepresentationGeneratedRegistrar::RegisterINT16LazyValueRepresentation(LazyValueRepresentationRegistryArguments args)
{
    return std::make_shared<INTLazyValueRepresentation>(args.valueAddress, args.size, args.type);
}

LazyValueRepresentationRegistryReturnType
LazyValueRepresentationGeneratedRegistrar::RegisterINT32LazyValueRepresentation(LazyValueRepresentationRegistryArguments args)
{
    return std::make_shared<INTLazyValueRepresentation>(args.valueAddress, args.size, args.type);
}

LazyValueRepresentationRegistryReturnType
LazyValueRepresentationGeneratedRegistrar::RegisterINT64LazyValueRepresentation(LazyValueRepresentationRegistryArguments args)
{
    return std::make_shared<INTLazyValueRepresentation>(args.valueAddress, args.size, args.type);
}
}
