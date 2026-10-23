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

#include <DataTypes/LazyValueData.hpp>

#include <cstdint>
#include <ostream>

#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/std/ostream.h>
#include <ErrorHandling.hpp>
#include <ValueDeserializer.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

[[nodiscard]] nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const LazyValueData& lazyValue)
{
    oss << "Size(" << lazyValue.size << "): ";
    for (nautilus::val<uint32_t> i = 0; i < lazyValue.size; ++i)
    {
        const nautilus::val<int> byte = readValueFromMemRef<int8_t>(lazyValue.ptr + i) & nautilus::val<int>(0xff);
        oss << nautilus::hex;
        oss.operator<<(byte);
        oss << " ";
    }
    return oss;
}

VarVal LazyValueData::deserializeValue() const
{
    return deserializer->deserializeToVarVal(ptr, size, *nullValues, ArenaRef{arena});
}

/// Default implementation of binary operations on rhs lazy values: The lazy value is deserialized and the function is applied to the underlying varval.
#define DEFAULT_OPERATOR_RHS_OVERRIDE(operatorFunction) \
    VarVal operatorFunction(const VarVal& other, const LazyValueData& rhs) \
    { \
        const VarVal deserializedVal = rhs.deserializeValue(); \
        return other.operatorFunction(deserializedVal); \
    }

/// Default implementation of binary operations on lhs lazy values: The lazy value is deserialized and the function is applied to the underlying varval.
#define DEFAULT_OPERATOR_LHS_OVERRIDE(opFunction) \
    VarVal LazyValueData::opFunction(const VarVal& other) const \
    { \
        const VarVal deserializedLazyVal = deserializeValue(); \
        return deserializedLazyVal.opFunction(other); \
    }

VarVal LazyValueData::operator!() const
{
    const VarVal deserializedLazyVal = deserializeValue();
    return deserializedLazyVal.operator!();
}

/// Create default implementations for all overridden functions
DEFAULT_OPERATOR_RHS_OVERRIDE(operator+);
DEFAULT_OPERATOR_RHS_OVERRIDE(operator-);
DEFAULT_OPERATOR_RHS_OVERRIDE(operator*);
DEFAULT_OPERATOR_RHS_OVERRIDE(operator/);
DEFAULT_OPERATOR_RHS_OVERRIDE(operator%);
DEFAULT_OPERATOR_RHS_OVERRIDE(operator==)
DEFAULT_OPERATOR_RHS_OVERRIDE(operator!=)
DEFAULT_OPERATOR_RHS_OVERRIDE(operator&&)
DEFAULT_OPERATOR_RHS_OVERRIDE(operator||)
DEFAULT_OPERATOR_RHS_OVERRIDE(operator<)
DEFAULT_OPERATOR_RHS_OVERRIDE(operator>)
DEFAULT_OPERATOR_RHS_OVERRIDE(operator<=)
DEFAULT_OPERATOR_RHS_OVERRIDE(operator>=)
DEFAULT_OPERATOR_RHS_OVERRIDE(operator&)
DEFAULT_OPERATOR_RHS_OVERRIDE(operator|)
DEFAULT_OPERATOR_RHS_OVERRIDE(operator^)
DEFAULT_OPERATOR_RHS_OVERRIDE(operator<<)
DEFAULT_OPERATOR_RHS_OVERRIDE(operator>>)

DEFAULT_OPERATOR_LHS_OVERRIDE(operator+);
DEFAULT_OPERATOR_LHS_OVERRIDE(operator-);
DEFAULT_OPERATOR_LHS_OVERRIDE(operator*);
DEFAULT_OPERATOR_LHS_OVERRIDE(operator/);
DEFAULT_OPERATOR_LHS_OVERRIDE(operator%)
DEFAULT_OPERATOR_LHS_OVERRIDE(operator==)
DEFAULT_OPERATOR_LHS_OVERRIDE(operator!=)
DEFAULT_OPERATOR_LHS_OVERRIDE(operator&&)
DEFAULT_OPERATOR_LHS_OVERRIDE(operator||)
DEFAULT_OPERATOR_LHS_OVERRIDE(operator<)
DEFAULT_OPERATOR_LHS_OVERRIDE(operator>)
DEFAULT_OPERATOR_LHS_OVERRIDE(operator<=)
DEFAULT_OPERATOR_LHS_OVERRIDE(operator>=)
DEFAULT_OPERATOR_LHS_OVERRIDE(operator&)
DEFAULT_OPERATOR_LHS_OVERRIDE(operator|)
DEFAULT_OPERATOR_LHS_OVERRIDE(operator^)
DEFAULT_OPERATOR_LHS_OVERRIDE(operator<<)
DEFAULT_OPERATOR_LHS_OVERRIDE(operator>>)
}
