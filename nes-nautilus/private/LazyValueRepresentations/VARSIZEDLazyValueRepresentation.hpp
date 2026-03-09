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
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{
class VarVal;

/// Since a Varsized Value is, by definition, lazy anyway, all applicable functions involving Varsized typed values can be overridden in this class.
/// While this does not save us any parsing optimization per se, we can avoid a cast this way.
class VARSIZEDLazyValueRepresentation final : public LazyValueRepresentation
{
public:
    explicit VARSIZEDLazyValueRepresentation(
        const nautilus::val<int8_t*>& reference, const nautilus::val<uint64_t>& size, const DataType type, const nautilus::val<bool>& isNull)
        : LazyValueRepresentation(reference, size, type, isNull)
    {
    }

    /// == Implementations
    [[nodiscard]] nautilus::val<bool> eqImpl(const nautilus::val<bool>& rhs) const;
    [[nodiscard]] nautilus::val<bool> eqImpl(const VariableSizedData& rhs) const;
    [[nodiscard]] nautilus::val<bool> eqImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const;

    /// != Implementations
    [[nodiscard]] nautilus::val<bool> neqImpl(const nautilus::val<bool>& rhs) const;
    [[nodiscard]] nautilus::val<bool> neqImpl(const VariableSizedData& rhs) const;
    [[nodiscard]] nautilus::val<bool> neqImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const;


    [[nodiscard]] VarVal operator!() const override;

    [[nodiscard]] VarVal operator==(const VarVal& other) const override;
    [[nodiscard]] VarVal reverseEQ(const VarVal& other) const override;
    [[nodiscard]] VarVal operator!=(const VarVal& other) const override;
    [[nodiscard]] VarVal reverseNEQ(const VarVal& other) const override;
};
}
