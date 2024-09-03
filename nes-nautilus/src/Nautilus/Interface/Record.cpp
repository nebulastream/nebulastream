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
#include <sstream>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Nautilus
{
Record::Record(std::unordered_map<RecordFieldIdentifier, VarVal>&& fields) : recordFields(fields)
{
}

bool Record::operator==(const Record& rhs) const
{
    if (recordFields.size() != rhs.recordFields.size())
    {
        return false;
    }

    for (const auto& [fieldIdentifier, value] : nautilus::static_iterable(recordFields))
    {
        if (!rhs.hasField(fieldIdentifier))
        {
            return false;
        }

        if (value != rhs.recordFields.at(fieldIdentifier))
        {
            return false;
        }
    }

    return true;
}

bool Record::operator!=(const Record& rhs) const
{
    return !(*this == rhs);
}

const VarVal& Record::read(const std::string& recordFieldIdentifier) const
{
    return recordFields.at(recordFieldIdentifier);
}
void Record::write(const RecordFieldIdentifier& recordFieldIdentifier, const VarVal& dataType)
{
    recordFields.insert_or_assign(recordFieldIdentifier, dataType);
}

std::vector<Record::RecordFieldIdentifier> Record::getAllFields()
{
    std::vector<RecordFieldIdentifier> fieldIdentifierVec;
    fieldIdentifierVec.reserve(recordFields.size());
    for (auto& [fieldIdentifier, value] : recordFields)
    {
        fieldIdentifierVec.emplace_back(fieldIdentifier);
    }

    return fieldIdentifierVec;
}

std::ostream& operator<<(std::ostream& os, const Record& record)
{
    for (const auto& [fieldIdentifier, value] : nautilus::static_iterable(record.recordFields))
    {
        os << fieldIdentifier << ": " << value << ", ";
    }
    return os;
}


uint64_t Record::numberOfFields() const
{
    return recordFields.size();
}

bool Record::hasField(const RecordFieldIdentifier& fieldName) const
{
    return recordFields.contains(fieldName);
}

}