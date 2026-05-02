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
#include <map>
#include <numeric>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <Nautilus/DataTypes/Value.hpp>
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

Value Record::read(const RecordFieldIdentifier& fieldName) const
{
    if (not recordFields.contains(fieldName))
    {
        const std::string allFields = std::accumulate(
            recordFields.begin(),
            recordFields.end(),
            std::string{},
            [](const std::string& acc, const auto& pair) { return acc + pair.first + ", "; });
        throw FieldNotFound("Field {} not found in record {}.", fieldName, allFields);
    }
    return Value::scalar(recordFields.at(fieldName));
}

Value Record::read(const RecordFieldIdentifier& fieldName, const std::vector<std::string>& suffixes) const
{
    std::map<std::string, VarVal> components;
    for (const auto& suffix : suffixes)
    {
        const auto fullName = fieldName + suffix;
        if (not recordFields.contains(fullName))
        {
            const std::string allFields = std::accumulate(
                recordFields.begin(),
                recordFields.end(),
                std::string{},
                [](const std::string& acc, const auto& pair) { return acc + pair.first + ", "; });
            throw FieldNotFound("Field {} not found in record {}.", fullName, allFields);
        }
        components.emplace(suffix, recordFields.at(fullName));
    }
    return Value{std::move(components)};
}

void Record::write(const RecordFieldIdentifier& fieldName, const Value& value)
{
    for (const auto& [suffix, component] : value.components())
    {
        const auto fullName = fieldName + suffix;
        /// We can not use the insert_or_assign method, as we otherwise run into a tracing exception, as this might result in incorrect code.
        if (const auto [hashMapIterator, inserted] = recordFields.insert({fullName, component}); not inserted)
        {
            /// We need to first erase the old value, as we are overwriting an existing field with a potential new data type
            /// This inefficiency is fine, as this code solely gets executed during tracing.
            recordFields.erase(fullName);
            recordFields.insert_or_assign(fullName, component);
        }
    }
}

void Record::write(const RecordFieldIdentifier& fieldName, const VarVal& varVal)
{
    write(fieldName, Value::scalar(varVal));
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
        if (not rhs.hasField(fieldName) or value != rhs.recordFields.at(fieldName))
        {
            return false;
        }
    }

    return true;
}

}
