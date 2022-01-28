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
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Formats/SinkFormat.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <utility>

namespace NES {

SinkFormat::SinkFormat(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager)
    : schema(std::move(schema)), bufferManager(std::move(bufferManager)) {}

SchemaPtr SinkFormat::getSchemaPtr() { return schema; }

void SinkFormat::setSchemaPtr(SchemaPtr schema) { this->schema = std::move(schema); }

Runtime::BufferManagerPtr SinkFormat::getBufferManager() { return bufferManager; }

void SinkFormat::setBufferManager(Runtime::BufferManagerPtr bufferManager) { this->bufferManager = std::move(bufferManager); }

}// namespace NES