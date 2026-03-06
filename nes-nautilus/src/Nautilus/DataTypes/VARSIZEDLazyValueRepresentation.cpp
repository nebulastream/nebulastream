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

#include <cstring>
#include <LazyValueRepresentations/VARSIZEDLazyValueRepresentation.hpp>
#include <nautilus/std/cstring.h>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <LazyValueRepresentationRegistry.hpp>
#include <val_bool.hpp>

namespace NES
{

VarVal VARSIZEDLazyValueRepresentation::eqImpl(const nautilus::val<bool>& rhs) const
{
    if (isNull)
    {
        return VarVal{false, true, true};
    }
    return isValid() == rhs;
}

VarVal VARSIZEDLazyValueRepresentation::eqImpl(const VariableSizedData& rhs) const
{
    if (isNull)
    {
        return VarVal{false, true, true};
    }
    if (size != rhs.getSize())
    {
        return nautilus::val<bool>(false);
    }
    const auto varSizedData = getContent();
    const auto rhsVarSizedData = rhs.getContent();
    const auto compareResult = (nautilus::memcmp(varSizedData, rhsVarSizedData, size) == 0);
    return {compareResult};
}

VarVal VARSIZEDLazyValueRepresentation::eqImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const
{
    if (isNull || rhs->getIsNull())
    {
        return VarVal{false, true, true};
    }
    if (rhs->getType().type == DataType::Type::BOOLEAN)
    {
        return rhs->isBooleanTrue() == isValid();
    }
    if (size != rhs->getSize())
    {
        return nautilus::val<bool>(false);
    }
    const auto varSizedData = getContent();
    const auto rhsVarSizedData = rhs->getContent();
    const auto compareResult = (nautilus::memcmp(varSizedData, rhsVarSizedData, size) == 0);
    return {compareResult};
}

VarVal VARSIZEDLazyValueRepresentation::neqImpl(const nautilus::val<bool>& rhs) const
{
    return isValid() != rhs;
}

VarVal VARSIZEDLazyValueRepresentation::neqImpl(const VariableSizedData& rhs) const
{
    return !eqImpl(rhs);
}

VarVal VARSIZEDLazyValueRepresentation::neqImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const
{
    return !eqImpl(rhs);
}

VarVal VARSIZEDLazyValueRepresentation::operator!() const
{
    return !isValid();
}

VarVal VARSIZEDLazyValueRepresentation::operator==(const VarVal& other) const
{
    return other.customVisit(
        [&]<typename T>(const T& val) -> VarVal
        {
            if constexpr (requires(T val) { this->eqImpl(val); })
            {
                return this->eqImpl(val);
            }
            return LazyValueRepresentation::operator==(val);
        });
}

VarVal VARSIZEDLazyValueRepresentation::reverseEQ(const VarVal& other) const
{
    return operator==(other);
}

VarVal VARSIZEDLazyValueRepresentation::operator!=(const VarVal& other) const
{
    return other.customVisit(
        [&]<typename T>(const T& val) -> VarVal
        {
            if constexpr (requires(T val) { this->neqImpl(val); })
            {
                return this->eqImpl(val);
            }
            return LazyValueRepresentation::operator==(val);
        });
}

VarVal VARSIZEDLazyValueRepresentation::reverseNEQ(const VarVal& other) const
{
    return other.customVisit(
        [&]<typename T>(const T& val) -> VarVal
        {
            if constexpr (requires(T val) { this->neqImpl(val); })
            {
                return this->eqImpl(val);
            }
            return LazyValueRepresentation::operator==(val);
        });
}

LazyValueRepresentationRegistryReturnType
LazyValueRepresentationGeneratedRegistrar::RegisterVARSIZEDLazyValueRepresentation(LazyValueRepresentationRegistryArguments args)
{
    return std::make_shared<VARSIZEDLazyValueRepresentation>(args.valueAddress, args.size, args.type, args.isNull);
}
}
