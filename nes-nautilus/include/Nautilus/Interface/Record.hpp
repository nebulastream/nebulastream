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

#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <map>
#include <memory>
#include <ostream>

namespace NES::Nautilus {

/**
 * @brief A record is the primitive abstraction of a single entry/tuple in a data set.
 * Operators receive records can read and write fields.
 */
class Record {
  public:
    using RecordFieldIdentifier = std::string;
    explicit Record();
    explicit Record(std::map<RecordFieldIdentifier, Value<>>&& fields);
    ~Record() = default;
    Value<>& read(RecordFieldIdentifier fieldName);
    void write(RecordFieldIdentifier fieldName, const Value<>& value);
    uint64_t numberOfFields();
    bool hasField(RecordFieldIdentifier fieldName);
    std::vector<RecordFieldIdentifier> getAllFields();
    std::string toString();
    bool operator==(const Record& rhs) const;
    bool operator!=(const Record& rhs) const;

  private:
    std::map<RecordFieldIdentifier, Value<>> fields;
};

}// namespace NES::Nautilus

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_RECORD_HPP_
