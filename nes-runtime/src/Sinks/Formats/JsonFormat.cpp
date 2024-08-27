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

#include <iostream>
#include <utility>
#include <API/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

std::string JsonFormat::getFormattedSchema()
{
    NES_NOT_IMPLEMENTED();
}

JsonFormat::JsonFormat(SchemaPtr schema, std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider)
    : SinkFormat(std::move(schema), std::move(bufferProvider))
{
}

std::string JsonFormat::getFormattedBuffer(Memory::TupleBuffer&)
{
    NES_NOT_IMPLEMENTED();
}

std::string JsonFormat::toString()
{
    return "JSON_FORMAT";
}
FormatTypes JsonFormat::getSinkFormat()
{
    return FormatTypes::JSON_FORMAT;
}

FormatIterator JsonFormat::getTupleIterator(Memory::TupleBuffer& inputBuffer)
{
    return FormatIterator(schema, inputBuffer, FormatTypes::JSON_FORMAT);
}

} /// namespace NES
