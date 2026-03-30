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

#include <SinksParsing/NoneFormat.hpp>

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SinksParsing/Format.hpp>
#include <fmt/format.h>

namespace NES
{
NoneFormat::NoneFormat(const Schema& schema) : Format(schema)
{
}

std::string NoneFormat::getFormattedBuffer(const TupleBuffer&) const
{
    return "";
}

std::ostream& operator<<(std::ostream& out, const NoneFormat& format)
{
    return out << fmt::format("CSVFormat(Schema: {})", format.schema);
}
}
