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
#include <Nautilus/Interface/Record.hpp>

#include <cstdint>
#include <numeric>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <std/ostream.h>
#include <ErrorHandling.hpp>
#include <static.hpp>
#include <val.hpp>

namespace NES::Nautilus
{
Record::Record(std::unordered_map<RecordFieldIdentifier, VarVal>&& fields) : recordFields(std::move(fields))
{
}

Record::Record(const Record& other) : recordFields(other.recordFields)
{
    // Intentionally do not copy lazy fields; they will either be re-registered by producers
    // or remain absent in the copy. This preserves logical state while keeping Record copyable.
}

Record& Record::operator=(const Record& other)
{
    if (this != &other)
    {
        recordFields = other.recordFields;
        // Drop any pending lazy materializers in the target
        lazyFields.clear();
    }
    return *this;
}

const VarVal& Record::read(const RecordFieldIdentifier& recordFieldIdentifier) const
{
    // Fast path: already materialized
    if (auto it = recordFields.find(recordFieldIdentifier); it != recordFields.end())
    {
        return it->second;
    }

    // If we have a lazy entry, materialize it on-demand
    if (auto lit = lazyFields.find(recordFieldIdentifier); lit != lazyFields.end())
    {
        LazyEntry& entry = lit->second;
        // Ensure single materialization in concurrent accesses
        std::call_once(entry.once, [&]() {
            VarVal v = entry.materializeFn();
            recordFields.emplace(recordFieldIdentifier, std::move(v));
            // Erase lazy entry after successful materialization
            lazyFields.erase(lit);
        });
        // After call_once, value must be present
        return recordFields.at(recordFieldIdentifier);
    }

    const std::string allFields = std::accumulate(
        recordFields.begin(),
        recordFields.end(),
        std::string{},
        [](const std::string& acc, const auto& pair) { return acc + pair.first + ", "; });
    throw FieldNotFound("Field {} not found in record {}.", recordFieldIdentifier, allFields);
}

void Record::write(const RecordFieldIdentifier& recordFieldIdentifier, const VarVal& varVal)
{
    // Ensure we drop any lazy entry with same name to avoid stale thunks
    if (auto it = lazyFields.find(recordFieldIdentifier); it != lazyFields.end())
    {
        lazyFields.erase(it);
    }

    /// We can not use the insert_or_assign method, as we otherwise run into a tracing exception, as this might result in incorrect code.
    if (const auto [hashMapIterator, inserted] = recordFields.insert({recordFieldIdentifier, varVal}); not inserted)
    {
        /// We need to first erase the old value, as we are overwriting an existing field with a potential new data type
        /// This inefficiency is fine, as this code solely gets executed during tracing.
        recordFields.erase(recordFieldIdentifier);
        recordFields.insert_or_assign(recordFieldIdentifier, varVal);
    }
}

void Record::writeLazy(const RecordFieldIdentifier& recordFieldIdentifier, MaterializerFn materializer)
{
    // Overwrite any existing lazy/materialized value with a new lazy thunk
    if (auto it = recordFields.find(recordFieldIdentifier); it != recordFields.end())
    {
        recordFields.erase(it);
    }
    // Remove existing lazy entry if present, then emplace a fresh one (once_flag is not assignable)
    if (auto lit = lazyFields.find(recordFieldIdentifier); lit != lazyFields.end())
    {
        lazyFields.erase(lit);
    }
    auto [it2, inserted2] = lazyFields.try_emplace(recordFieldIdentifier);
    it2->second.materializeFn = std::move(materializer);
}

void Record::materialize(const RecordFieldIdentifier& recordFieldIdentifier) const
{
    (void)read(recordFieldIdentifier);
}

void Record::materializeAll() const
{
    // Snapshot keys to avoid iterator invalidation while erasing
    std::vector<RecordFieldIdentifier> keys;
    keys.reserve(lazyFields.size());
    for (const auto& kv : lazyFields)
    {
        keys.emplace_back(kv.first);
    }
    for (const auto& k : keys)
    {
        (void)read(k);
    }
}

void Record::reassignFields(const Record& other)
{
    for (const auto& [fieldIdentifier, value] : nautilus::static_iterable(other.recordFields))
    {
        write(fieldIdentifier, value);
    }
}

nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& os, const Record& record)
{
    for (const auto& [_, value] : nautilus::static_iterable(record.recordFields))
    {
        os << value << ", ";
    }
    return os;
}

nautilus::val<uint64_t> Record::getNumberOfFields() const
{
    return recordFields.size();
}

bool Record::operator==(const Record& rhs) const
{
    return recordFields == rhs.recordFields;
}

bool Record::operator!=(const Record& rhs) const
{
    return !(rhs == *this);
}

}
