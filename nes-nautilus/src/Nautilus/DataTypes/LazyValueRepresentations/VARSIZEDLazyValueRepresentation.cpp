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

#include <cstring>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <nautilus/std/cstring.h>
#include <LazyValueRepresentationRegistry.hpp>
#include <select.hpp>
#include <val_bool.hpp>

namespace NES
{

nautilus::val<bool> VARSIZEDLazyValueRepresentation::eqImpl(const nautilus::val<bool>& rhs) const
{
    return isValid() == rhs;
}

nautilus::val<bool> VARSIZEDLazyValueRepresentation::eqImpl(const VariableSizedData& rhs) const
{
    if (size != rhs.getSize())
    {
        return nautilus::val<bool>(false);
    }
    const auto varSizedData = getContent();
    const auto rhsVarSizedData = rhs.getContent();
    const auto compareResult = (nautilus::memcmp(varSizedData, rhsVarSizedData, size) == 0);
    return compareResult;
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
        const auto varSizedData = getContent();
        const auto rhsVarSizedData = rhs->getContent();
        result = (nautilus::memcmp(varSizedData, rhsVarSizedData, size) == 0);
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
        [this, leftIsNullable = this->type.nullable,
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
            return LazyValueRepresentation::operator==({val,rightIsNullable,rightIsNull});
        });
}

VarVal VARSIZEDLazyValueRepresentation::reverseEQ(const VarVal& other) const
{
    return operator==(other);
}

VarVal VARSIZEDLazyValueRepresentation::operator!=(const VarVal& other) const
{
    return other.customVisit(
        [this, leftIsNullable = this->type.nullable,
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
            return LazyValueRepresentation::operator!=({val,rightIsNullable,rightIsNull});
        });
}

VarVal VARSIZEDLazyValueRepresentation::reverseNEQ(const VarVal& other) const
{
    return operator!=(other);
}

LazyValueRepresentationRegistryReturnType
LazyValueRepresentationGeneratedRegistrar::RegisterVARSIZEDLazyValueRepresentation(LazyValueRepresentationRegistryArguments args)
{
    return std::make_shared<VARSIZEDLazyValueRepresentation>(args.valueAddress, args.size, args.type, args.isNull);
}
}
