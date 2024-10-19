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

SinkFormat::SinkFormat(SchemaPtr schema) : SinkFormat(schema, false)
{
}

SinkFormat::SinkFormat(SchemaPtr schema, bool addTimestamp) : schema(std::move(schema)), addTimestamp(addTimestamp)
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

std::shared_ptr<Memory::AbstractBufferProvider> SinkFormat::getBufferManager()
{
    return bufferProvider;
}

void SinkFormat::setBufferManager(std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider)
{
    this->bufferProvider = std::move(bufferProvider);
}
bool SinkFormat::getAddTimestamp()
{
    return addTimestamp;
}
void SinkFormat::setAddTimestamp(bool addTimestamp)
{
    this->addTimestamp = addTimestamp;
}
}
