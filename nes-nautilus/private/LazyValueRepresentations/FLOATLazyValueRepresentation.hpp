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
/// Intermediate LazyValueRepresentation class which every signed float type's LazyValueRepresentation inherits from.
/// Overrides a collection of logical functions which can be performed on lazy integer values.
class FLOATLazyValueRepresentation : public LazyValueRepresentation
{
public:
    explicit FLOATLazyValueRepresentation(
        const nautilus::val<int8_t*>& reference, const nautilus::val<uint64_t>& size, const DataType type, const nautilus::val<bool>& isNull)
        : LazyValueRepresentation(reference, size, type, isNull)
    {
    }

    /// == Implementations
    [[nodiscard]] nautilus::val<bool> eqImpl(const nautilus::val<bool>& rhs) const;
    [[nodiscard]] nautilus::val<bool> eqImpl(const VariableSizedData& rhs) const;
    [[nodiscard]] nautilus::val<bool> eqImpl(const std::shared_ptr<LazyValueRepresentation>& rhs) const;
    [[nodiscard]] nautilus::val<bool> eqImpl(const nautilus::val<int8_t>& rhs) const;
    [[nodiscard]] nautilus::val<bool> eqImpl(const nautilus::val<int16_t>& rhs) const;
    [[nodiscard]] nautilus::val<bool> eqImpl(const nautilus::val<int32_t>& rhs) const;
    [[nodiscard]] nautilus::val<bool> eqImpl(const nautilus::val<int64_t>& rhs) const;
    [[nodiscard]] nautilus::val<bool> eqImpl(const nautilus::val<uint8_t>& rhs) const;
    [[nodiscard]] nautilus::val<bool> eqImpl(const nautilus::val<uint16_t>& rhs) const;
    [[nodiscard]] nautilus::val<bool> eqImpl(const nautilus::val<uint32_t>& rhs) const;
    [[nodiscard]] nautilus::val<bool> eqImpl(const nautilus::val<uint64_t>& rhs) const;



    [[nodiscard]] VarVal operator==(const VarVal& other) const override;
    [[nodiscard]] VarVal reverseEQ(const VarVal& other) const override;
    [[nodiscard]] VarVal operator!=(const VarVal& other) const override;
    [[nodiscard]] VarVal reverseNEQ(const VarVal& other) const override;
};
}
