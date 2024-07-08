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
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <sstream>

namespace NES::Nautilus {
const DataType& Record::read(const std::string& recordFieldIdentifier) const {
    return recordFields.at(recordFieldIdentifier);
}
void Record::write(const Record::RecordFieldIdentifier& recordFieldIdentifier, const DataType& dataType) {
    recordFields[recordFieldIdentifier] = dataType;
}

std::vector<Record::RecordFieldIdentifier> Record::getAllFields() {
    std::vector<Record::RecordFieldIdentifier> fieldIdentifierVec;
    fieldIdentifierVec.reserve(recordFields.size());
for (auto& [fieldIdentifier, value] : recordFields) {
        fieldIdentifierVec.emplace_back(fieldIdentifier);
    }

    return fieldIdentifierVec;
}

std::string Record::toString() {
    // Figure out later why this does not seem to work.
//    return fmt::format("{}", fmt::join(recordFields, ", "));
    NES_NOT_IMPLEMENTED();
}

uint64_t Record::numberOfFields() const { return recordFields.size(); }
bool Record::hasField(const Record::RecordFieldIdentifier& fieldName) { return recordFields.contains(fieldName); }


}// namespace NES::Nautilus
