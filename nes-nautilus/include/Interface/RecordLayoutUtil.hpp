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
#include <functional>
#include <ranges>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/Record.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Describes one field of a record as the layout sees it: its identity, its type, and the memory
/// address of its slot. A memory layout (row, column, paged vector, ...) differs from the others
/// only in how it resolves these addresses, so the shared read/write loops below operate purely on
/// a list of FieldAccess and stay layout agnostic.
struct FieldAccess
{
    Record::RecordFieldIdentifier name;
    DataType dataType;
    nautilus::val<int8_t*> address;

    /// Builds the addressed-field list by mapping every field through @param calcFieldAccess. Used by write paths,
    /// which address every field (writeRecordFields then skips fields absent from the record).
    template <typename FieldRange, typename CalcFieldAccessFn>
    static std::vector<FieldAccess> createFieldAccesses(FieldRange&& fields, const CalcFieldAccessFn& calcFieldAccess)
    {
        return std::forward<FieldRange>(fields) | std::views::transform(calcFieldAccess) | std::ranges::to<std::vector>();
    }

    /// Builds the addressed-field list, keeping only the fields accepted by @param includesField (e.g. projections)
    /// and mapping each survivor through @param calcFieldAccess.
    template <typename FieldRange, typename CalcFieldAccessFn, typename IncludesFieldFn>
    static std::vector<FieldAccess>
    createFieldAccesses(FieldRange&& fields, const CalcFieldAccessFn& calcFieldAccess, const IncludesFieldFn& includesField)
    {
        return std::forward<FieldRange>(fields) | std::views::filter(includesField) | std::views::transform(calcFieldAccess)
            | std::ranges::to<std::vector>();
    }
};

/// Variable-sized seam used only for the VARSIZED branch, since varsized payloads live outside the
/// fixed-size slot (in a PagedVector page or a TupleBuffer child buffer, depending on the layout).
/// Load resolves the (pointer, length) of the payload; store writes the payload at the slot.
using VarSizedLoadFn = std::function<std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>(nautilus::val<int8_t*> slot)>;
using VarSizedStoreFn = std::function<void(nautilus::val<int8_t*> slot, const VarVal& value)>;

/// Materializes a record from the given already-addressed fields. The caller decides which fields to
/// pass (e.g. after applying projections), so every field in @param fields is read.
Record readRecordFields(const std::vector<FieldAccess>& fields, const VarSizedLoadFn& loadVarSized);

/// Serializes the record's fields into their slots. Fields not present in @param record are skipped.
void writeRecordFields(const std::vector<FieldAccess>& fields, const Record& record, const VarSizedStoreFn& storeVarSized);

}
