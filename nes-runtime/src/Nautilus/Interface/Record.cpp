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

namespace NES::Nautilus {

Record::Record() {}

Record::Record(std::map<RecordFieldIdentifier, Value<>>&& fields) : fields(std::move(fields)) {}

Value<Any>& Record::read(RecordFieldIdentifier fieldIdentifier) {
    auto fieldValue = fields.find(fieldIdentifier);
    if (fieldValue == fields.end()) {
        throw new InterpreterException("Field " + fieldIdentifier + " does not exists");
    }
    return fieldValue->second;
}

uint64_t Record::numberOfFields() { return fields.size(); }

void Record::write(RecordFieldIdentifier fieldIndex, Value<Any>& value) { fields.insert_or_assign(fieldIndex, value); }

bool Record::hasField(NES::Nautilus::Record::RecordFieldIdentifier fieldName) { return fields.contains(fieldName); }
std::vector<Record::RecordFieldIdentifier> Record::getAllFields() {
    std::vector<Record::RecordFieldIdentifier> fieldIdentifierVec;
    for (auto& [fieldIdentifier, value] : fields) {
        fieldIdentifierVec.emplace_back(fieldIdentifier);
    }

    return fieldIdentifierVec;
}
}// namespace NES::Nautilus