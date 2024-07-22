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

#include <utility>
#include <API/Schema.hpp>
#include <Sinks/Formats/SinkFormat.hpp>

namespace NES
{

SinkFormat::SinkFormat(SchemaPtr schema, std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager) : SinkFormat(schema, bufferManager, false)
{
}

SinkFormat::SinkFormat(SchemaPtr schema, std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager, bool addTimestamp)
    : schema(std::move(schema)), bufferManager(std::move(bufferManager)), addTimestamp(addTimestamp)
{
}

SchemaPtr SinkFormat::getSchemaPtr()
{
    return schema;
}

void SinkFormat::setSchemaPtr(SchemaPtr schema)
{
    this->schema = std::move(schema);
}

std::shared_ptr<Runtime::AbstractBufferProvider> SinkFormat::getBufferManager()
{
    return bufferManager;
}

void SinkFormat::setBufferManager(std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager)
{
    this->bufferManager = std::move(bufferManager);
}
bool SinkFormat::getAddTimestamp()
{
    return addTimestamp;
}
void SinkFormat::setAddTimestamp(bool addTimestamp)
{
    this->addTimestamp = addTimestamp;
}
} /// namespace NES
