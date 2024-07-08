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

#ifndef NES_NES_NAUTILUS_NEW_INCLUDE_INTERFACE_RECORD_HPP_
#define NES_NES_NAUTILUS_NEW_INCLUDE_INTERFACE_RECORD_HPP_

#include <Nautilus/DataTypes/ExecutableDataType.hpp>
#include <unordered_map>

namespace NES::Nautilus {

class Record {
  public:
    using RecordFieldIdentifier = std::string;
    explicit Record();
    explicit Record(std::unordered_map<RecordFieldIdentifier, DataType>&& fields);
    ~Record() = default;

    const DataType& read(const RecordFieldIdentifier& recordFieldIdentifier) const;
    void write(const RecordFieldIdentifier& recordFieldIdentifier, const DataType& dataType);
    uint64_t numberOfFields() const;
    bool hasField(const RecordFieldIdentifier& fieldName);
    std::vector<RecordFieldIdentifier> getAllFields();
    std::string toString();
    bool operator==(const Record& rhs) const;
    bool operator!=(const Record& rhs) const;
  private:
    std::unordered_map<RecordFieldIdentifier, DataType> recordFields;
};

}// namespace NES::Nautilus

#endif//NES_NES_NAUTILUS_NEW_INCLUDE_INTERFACE_RECORD_HPP_
