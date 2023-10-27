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
#include <Sinks/Formats/FormatIterators/FormatIterator.hpp>
#include <utility>

namespace NES {

FormatIterator::FormatIterator(SchemaPtr schema, Runtime::TupleBuffer buffer, FormatTypes formatType)
    : schema(std::move(schema)), buffer(std::move(buffer)), sinkFormatType(formatType){};
}// namespace NES
