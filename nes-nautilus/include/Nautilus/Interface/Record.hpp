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
#include <Nautilus/DataTypes/VarVal.hpp>

namespace NES::Nautilus
{

/**
 * @brief A record is the primitive abstraction of a single entry/tuple in a data set.
 * Operators receive records can read and write fields.
 */
class Record
{
public:
    using RecordFieldIdentifier = std::string;
    explicit Record() = default;
    explicit Record(std::unordered_map<RecordFieldIdentifier, VarVal>&& recordFields);
    ~Record() = default;

    const VarVal& read(const RecordFieldIdentifier& recordFieldIdentifier) const;
    void write(const RecordFieldIdentifier& recordFieldIdentifier, const VarVal& dataType);
    nautilus::val<uint64_t> getNumberOfFields() const;

    friend nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& os, const Record& record);
    bool operator==(const Record& rhs) const;
    bool operator!=(const Record& rhs) const;

private:
    std::unordered_map<RecordFieldIdentifier, VarVal> recordFields;
};

}
