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

#include <API/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <utility>

namespace NES {

JsonFormat::JsonFormat(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager)
    : SinkFormat(std::move(schema), std::move(bufferManager)) {}

std::vector<Runtime::TupleBuffer> JsonFormat::getData(Runtime::TupleBuffer&) { NES_NOT_IMPLEMENTED(); }

std::optional<Runtime::TupleBuffer> JsonFormat::getSchema() { NES_NOT_IMPLEMENTED(); }

std::string JsonFormat::toString() { return "JSON_FORMAT"; }
FormatTypes JsonFormat::getSinkFormat() { return JSON_FORMAT; }

FormatIterator JsonFormat::getTupleIterator(Runtime::TupleBuffer& inputBuffer) {
    return FormatIterator(schema, inputBuffer, JSON_FORMAT);
}

}// namespace NES