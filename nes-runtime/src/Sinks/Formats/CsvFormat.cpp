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

#include <API/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>
#include <regex>
#include <utility>

namespace NES {

CsvFormat::CsvFormat(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager) : CsvFormat(schema, bufferManager, false) {}

CsvFormat::CsvFormat(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager, bool addTimestamp)
    : SinkFormat(schema, bufferManager, addTimestamp) {}

std::string CsvFormat::getFormattedSchema() {
    std::string out = Util::toCSVString(schema);
    if (addTimestamp) {
        out = Util::trimWhiteSpaces(out);
        out.append(",arr_timestamp\n");
    }
    return out;
}

std::string CsvFormat::getFormattedBuffer(Runtime::TupleBuffer& inputBuffer) {
    std::string bufferContent;
    if (addTimestamp) {
        auto timestamp = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        bufferContent = Util::printTupleBufferAsCSV(inputBuffer, schema, "," + std::to_string(timestamp));
    } else {
        bufferContent = Util::printTupleBufferAsCSV(inputBuffer, schema);
    }
    return bufferContent;
}

std::string CsvFormat::toString() { return "CSV_FORMAT"; }

FormatTypes CsvFormat::getSinkFormat() { return FormatTypes::CSV_FORMAT; }

FormatIterator CsvFormat::getTupleIterator(Runtime::TupleBuffer& inputBuffer) {
    return FormatIterator(schema, inputBuffer, FormatTypes::CSV_FORMAT);
}

}// namespace NES
