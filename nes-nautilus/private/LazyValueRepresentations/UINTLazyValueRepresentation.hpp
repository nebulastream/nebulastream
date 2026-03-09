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

#include <cstdint>
#include <memory>
#include <Nautilus/DataTypes/LazyValueRepresentation.hpp>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <nautilus/val_concepts.hpp>
#include <RawValueParser.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{
class VarVal;

/// Intermediate LazyValueRepresentation class which every unsigned integer type's LazyValueRepresentation inherits from.
/// Overrides a collection of logical functions which can be performed on lazy integer values.
class UINTLazyValueRepresentation : public LazyValueRepresentation
{
public:
    explicit UINTLazyValueRepresentation(
        const nautilus::val<int8_t*>& reference, const nautilus::val<uint64_t>& size, const DataType::Type type)
        : LazyValueRepresentation(reference, size, type)
    {
    }

    /// == Implementations
    [[nodiscard]] VarVal eqImpl(const nautilus::val<bool>& rhs) const;
    [[nodiscard]] VarVal eqImpl(const VariableSizedData& rhs) const;
    /*
    template<typename T>
    requires nautilus::is_arithmetic<T>
    [[nodiscard]] VarVal eqImpl(const nautilus::val<T>& rhs) const
    {
        /// Transform rhs into a LazyValue and use it for the comparison
    }
    */
    [[nodiscard]] VarVal eqImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const;

    /// != Implementations
    [[nodiscard]] VarVal neqImpl(const nautilus::val<bool>& rhs) const;
    [[nodiscard]] VarVal neqImpl(const VariableSizedData& rhs) const;
    /*
    template<typename T>
    requires nautilus::is_arithmetic<T>
    [[nodiscard]] VarVal neqImpl(const nautilus::val<T>& rhs) const
    {
        /// Transform rhs into a LazyValue and use it for the comparison
    }
    */
    [[nodiscard]] VarVal neqImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const;

    /// < Implementations
    /*
    template<typename T>
    requires nautilus::is_arithmetic<T>
    [[nodiscard]] VarVal ltImpl(const nautilus::val<T>& rhs) const
    {
        /// Transform rhs into a LazyValue and use it for the comparison
    }
    */
    [[nodiscard]] VarVal ltImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const;

    /// <= Implementations
    /*
    template<typename T>
    requires nautilus::is_arithmetic<T>
    [[nodiscard]] VarVal leImpl(const nautilus::val<T>& rhs) const
    {
        /// Transform rhs into a LazyValue and use it for the comparison
    }
    */
    [[nodiscard]] VarVal leImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const;

    /// > Implementations
    /*
    template<typename T>
    requires nautilus::is_arithmetic<T>
    [[nodiscard]] VarVal gtImpl(const nautilus::val<T>& rhs) const
    {
        /// Transform rhs into a LazyValue and use it for the comparison
    }
    */
    [[nodiscard]] VarVal gtImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const;

    /// >= Implementations
    /*
    template<typename T>
    requires nautilus::is_arithmetic<T>
    [[nodiscard]] VarVal geImpl(const nautilus::val<T>& rhs) const
    {
        /// Transform rhs into a LazyValue and use it for the comparison
    }

    */
    [[nodiscard]] VarVal geImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const;

    [[nodiscard]] VarVal operator==(const VarVal& other) const override;
    [[nodiscard]] VarVal reverseEQ(const VarVal& other) const override;
    [[nodiscard]] VarVal operator!=(const VarVal& other) const override;
    [[nodiscard]] VarVal reverseNEQ(const VarVal& other) const override;
    [[nodiscard]] VarVal operator<(const VarVal& other) const override;
    [[nodiscard]] VarVal reverseLT(const VarVal& other) const override;
    [[nodiscard]] VarVal operator<=(const VarVal& other) const override;
    [[nodiscard]] VarVal reverseLE(const VarVal& other) const override;
    [[nodiscard]] VarVal operator>(const VarVal& other) const override;
    [[nodiscard]] VarVal reverseGT(const VarVal& other) const override;
    [[nodiscard]] VarVal operator>=(const VarVal& other) const override;
    [[nodiscard]] VarVal reverseGE(const VarVal& other) const override;
};
}
