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

#include <algorithm>
#include <numeric>
#include <sstream>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Util.hpp>
#include <ErrorHandling.hpp>
#include <static.hpp>

namespace NES::Nautilus
{
Record::Record(std::unordered_map<RecordFieldIdentifier, VarVal>&& fields) : recordFields(fields)
{
}

const VarVal& Record::read(const RecordFieldIdentifier& recordFieldIdentifier) const
{
    if (not recordFields.contains(recordFieldIdentifier))
    {
        const std::string allFields = std::accumulate(
            recordFields.begin(),
            recordFields.end(),
            std::string{},
            [](const std::string& acc, const auto& pair) { return acc + pair.first + ", "; });
        throw FieldNotFound("Field {} not found in record {}.", recordFieldIdentifier, allFields);
    }
    return recordFields.at(recordFieldIdentifier);
}

void Record::write(const RecordFieldIdentifier& recordFieldIdentifier, const VarVal& dataType)
{
    /// We can not use the insert_or_assign method, as we otherwise run into a nautilus tracing exception.
    if (const auto [hashMapIterator, inserted] = recordFields.insert({recordFieldIdentifier, dataType}); not inserted)
    {
        hashMapIterator->second = dataType;
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
