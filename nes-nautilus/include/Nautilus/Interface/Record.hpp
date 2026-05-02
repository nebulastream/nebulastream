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

#include <string>
#include <unordered_map>
#include <vector>
#include <Nautilus/DataTypes/Value.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>

namespace NES
{

/// A record is the primitive abstraction of a single entry/tuple in a data set.
///
/// Storage is flat — each entry is one VarVal keyed by primitive field name. The
/// public read/write API is in terms of Value, so callers don't need to know
/// whether they're working with a scalar or a compound logical type:
///   - read(name)               -> scalar Value (single component, key SCALAR_KEY)
///   - read(name, suffixes)     -> compound Value gathered across name+suffix
///   - write(name, value)       -> spreads each component into name+suffix
///
/// After the logical-lowering phase, every schema field is primitive, so most
/// fields hold scalar Values. Compound Values exist only as transients passing
/// through compound physical functions.
class Record
{
public:
    using RecordFieldIdentifier = std::string;
    explicit Record() = default;
    explicit Record(std::unordered_map<RecordFieldIdentifier, VarVal>&& recordFields);
    ~Record() = default;

    /// Adds all fields from the other record to this record. This will overwrite existing fields.
    void reassignFields(const Record& other);

    /// Read a single primitive field as a scalar Value.
    [[nodiscard]] Value read(const RecordFieldIdentifier& fieldName) const;

    /// Read a (possibly compound) Value: gathers `fieldName + suffix` for each suffix.
    /// For scalar reads pass {""}; for Point pass {".x", ".y", ".z"}.
    [[nodiscard]] Value read(const RecordFieldIdentifier& fieldName, const std::vector<std::string>& suffixes) const;

    /// Write a Value to the record. Each component is spread into `fieldName + suffix`.
    /// For a scalar Value (suffix = ""), this writes to `fieldName`.
    void write(const RecordFieldIdentifier& fieldName, const Value& value);

    /// Convenience overload for callers that already have a raw VarVal — wraps it as a scalar Value.
    void write(const RecordFieldIdentifier& fieldName, const VarVal& varVal);

    nautilus::val<uint64_t> getNumberOfFields() const;
    [[nodiscard]] bool hasField(const RecordFieldIdentifier& fieldName) const;

    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& os, const Record& record);
    friend nautilus::val<bool> operator==(const Record& lhs, const Record& rhs);

    friend nautilus::val<bool> operator!=(const Record& lhs, const Record& rhs) { return !(lhs == rhs); }

private:
    std::unordered_map<RecordFieldIdentifier, VarVal> recordFields;
};

}
