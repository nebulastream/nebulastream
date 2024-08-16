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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_RECORD_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_RECORD_HPP_

#include <Nautilus/DataTypes/AbstractDataType.hpp>
#include <unordered_map>

namespace NES::Nautilus {

class Record {
  public:
    using RecordFieldIdentifier = std::string;
    explicit Record() = default;
    explicit Record(std::unordered_map<RecordFieldIdentifier, ExecDataType>&& fields);
    ~Record() = default;

    const ExecDataType& read(const RecordFieldIdentifier& recordFieldIdentifier) const;
    void write(const RecordFieldIdentifier& recordFieldIdentifier, const ExecDataType& dataType);
    uint64_t numberOfFields() const;
    bool hasField(const RecordFieldIdentifier& fieldName) const;
    std::vector<RecordFieldIdentifier> getAllFields();

    // Get rid of this toString and override the << and the formatter
    std::string toString();
    bool operator==(const Record& rhs) const;
    bool operator!=(const Record& rhs) const;
  private:
    std::unordered_map<RecordFieldIdentifier, ExecDataType> recordFields;
};

}// namespace NES::Nautilus

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_RECORD_HPP_
