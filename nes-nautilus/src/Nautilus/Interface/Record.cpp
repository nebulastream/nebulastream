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
#include <Nautilus/DataTypes/VarVal.hpp>
#include <std/ostream.h>
#include <ErrorHandling.hpp>
#include <static.hpp>
#include <val.hpp>

namespace NES
{
Record::Record(std::unordered_map<RecordFieldIdentifier, VarVal>&& fields) : recordFields(fields)
{
}

const VarVal& Record::read(const RecordFieldIdentifier& recordFieldIdentifier) const
{
    auto fieldIt = recordFields.find(recordFieldIdentifier);
    if (fieldIt == recordFields.end())
    {
        const std::string allFields = std::accumulate(
            recordFields.begin(),
            recordFields.end(),
            std::string{},
            [](const std::string& acc, const auto& pair) { return acc + pair.first + ", "; });
        throw FieldNotFound("Field {} not found in record {}.", recordFieldIdentifier, allFields);
    }
    return fieldIt->second;
}

VarVal& Record::at(const RecordFieldIdentifier& recordFieldIdentifier)
{
    return recordFields.at(recordFieldIdentifier);
}

VarVal Record::exchange(const RecordFieldIdentifier& recordFieldIdentifier, const VarVal& newValue)
{
    auto fieldIt = recordFields.find(recordFieldIdentifier);
    if (fieldIt == recordFields.end())
    {
        const std::string allFields = std::accumulate(
            recordFields.begin(),
            recordFields.end(),
            std::string{},
            [](const std::string& acc, const auto& pair) { return acc + pair.first + ", "; });
        throw FieldNotFound("Field {} not found in record {}.", recordFieldIdentifier, allFields);
    }

    VarVal oldValue = fieldIt->second;
    fieldIt->second = newValue;
    return oldValue;
}

void Record::write(const RecordFieldIdentifier& recordFieldIdentifier, const VarVal& varVal)
{
    recordFields.insert_or_assign(recordFieldIdentifier, varVal);
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

bool Record::hasField(const RecordFieldIdentifier& fieldName) const
{
    return recordFields.contains(fieldName);
}

nautilus::val<bool> operator==(const Record& lhs, const Record& rhs)
{
    if (lhs.recordFields.size() != rhs.recordFields.size())
    {
        return false;
    }

    for (const auto& [fieldName, value] : nautilus::static_iterable(lhs.recordFields))
    {
        if (not rhs.hasField(fieldName) or value != rhs.read(fieldName))
        {
            return false;
        }
    }

    return true;
}

}
