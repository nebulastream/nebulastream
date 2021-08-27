/*

     Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <QueryCompiler/Interpreter/Record.hpp>
#include <QueryCompiler/Interpreter/Values/NesInt32.hpp>
#include <QueryCompiler/Interpreter/Values/NesValue.hpp>
#include <Util/Logger.hpp>
#include <utility>
namespace NES::QueryCompilation {
Record::Record(std::vector<NesValuePtr> records) : records(std::move(records)) {}

std::shared_ptr<NesValue> Record::read(std::string fieldName) {
    NES_DEBUG("Read: " << fieldName);
    return records[0];
}

void Record::write(std::string fieldName, std::shared_ptr<NesValue>) { NES_DEBUG("Write: " << fieldName); }



std::ostream& operator<<(std::ostream& os, const RecordPtr& r){
    os << "[";
    for (auto value : r.records) {
        os << value << ", ";
    }
    os << "]";
    return os;
}
}// namespace NES::QueryCompilation