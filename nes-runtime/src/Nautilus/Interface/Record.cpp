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

#include <Nautilus/Exceptions/InterpreterException.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <algorithm>
#include <sstream>

namespace NES::Nautilus {

Record::Record() {}

Record::Record(std::map<RecordFieldIdentifier, Value<>>&& fields) : fields(std::move(fields)) {}

Value<Any>& Record::read(RecordFieldIdentifier fieldIdentifier) {
    auto fieldsMapIterator = fields.find(fieldIdentifier);
    if (fieldsMapIterator == fields.end()) {
        std::stringstream ss;
        std::for_each(fields.begin(), fields.end(), [&ss](const auto& entry) {
            ss << entry.first;
            ss << ", ";
        });
        throw InterpreterException("Could not find field: fieldIdentifier = " + fieldIdentifier + "; known fields = " + ss.str());
    } else {
        return fieldsMapIterator->second;
    }
}

uint64_t Record::numberOfFields() { return fields.size(); }

void Record::write(RecordFieldIdentifier fieldIndex, const Value<Any>& value) { fields.insert_or_assign(fieldIndex, value); }

bool Record::hasField(NES::Nautilus::Record::RecordFieldIdentifier fieldName) { return fields.contains(fieldName); }

std::vector<Record::RecordFieldIdentifier> Record::getAllFields() {
    std::vector<Record::RecordFieldIdentifier> fieldIdentifierVec;
    for (auto& [fieldIdentifier, value] : fields) {
        fieldIdentifierVec.emplace_back(fieldIdentifier);
    }

    return fieldIdentifierVec;
}

std::string Record::toString() {
    std::ostringstream stringStream;
    for (auto& [fieldIdentifier, value] : fields) {
        stringStream << fieldIdentifier << ": " << value << ", ";
    }
    auto tmpStr = stringStream.str();
    tmpStr.pop_back();
    tmpStr.pop_back();

    return tmpStr;
}

bool Record::operator==(const Record& rhs) const { return fields == rhs.fields; }

bool Record::operator!=(const Record& rhs) const { return !(rhs == *this); }
}// namespace NES::Nautilus