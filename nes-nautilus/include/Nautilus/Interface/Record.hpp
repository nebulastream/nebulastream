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

#include <unordered_map>
#include <functional>
#include <mutex>

#include <Nautilus/DataTypes/VarVal.hpp>

namespace NES::Nautilus
{

/// A record is the primitive abstraction of a single entry/tuple in a data set.
/// Operators receiving records can read and write fields of the record.
class Record
{
public:
    using RecordFieldIdentifier = std::string;

    // Function that, when invoked, parses and returns the field value as VarVal.
    using MaterializerFn = std::function<VarVal()>;

    explicit Record() = default;
    explicit Record(std::unordered_map<RecordFieldIdentifier, VarVal>&& recordFields);
    // Custom copy semantics: copy only materialized fields; lazy thunks are not copied.
    Record(const Record& other);
    Record& operator=(const Record& other);
    // Move semantics: defaulted.
    Record(Record&&) noexcept = default;
    Record& operator=(Record&&) noexcept = default;
    ~Record() = default;

    /// Adds all fields from the other record to this record. This will overwrite existing fields.
    void reassignFields(const Record& other);

    /// Returns a reference to a materialized field; if the field is lazy, it is materialized on-demand.
    const VarVal& read(const RecordFieldIdentifier& recordFieldIdentifier) const;

    /// Write/overwrite a materialized value.
    void write(const RecordFieldIdentifier& recordFieldIdentifier, const VarVal& varVal);

    /// Register a lazy field with a materializer closure. The closure must be idempotent and thread-safe to call once.
    void writeLazy(const RecordFieldIdentifier& recordFieldIdentifier, MaterializerFn materializer);

    /// Ensure the given field is materialized (no-op if already materialized).
    void materialize(const RecordFieldIdentifier& recordFieldIdentifier) const;

    /// Materialize all lazy fields.
    void materializeAll() const;

    /// Returns the number of currently materialized fields (does not force materialization).
    nautilus::val<uint64_t> getNumberOfFields() const;

    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& os, const Record& record);
    bool operator==(const Record& rhs) const;
    bool operator!=(const Record& rhs) const;

private:
    struct LazyEntry {
        MaterializerFn materializeFn;
        mutable std::once_flag once;
    };

    // Mark caches as mutable to allow logical-const materialization inside const methods like read().
    mutable std::unordered_map<RecordFieldIdentifier, VarVal> recordFields;
    mutable std::unordered_map<RecordFieldIdentifier, LazyEntry> lazyFields;
};

}
