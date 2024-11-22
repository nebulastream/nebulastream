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

#pragma once
#include <API/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/ostream.h>

namespace NES::Sinks
{

class CSVFormat
{
public:
    CSVFormat(std::shared_ptr<Schema> schema, bool addTimestamp = false);
    virtual ~CSVFormat() noexcept = default;

    /// Returns the schema of formatted according to the specific SinkFormat represented as string.
    std::string getFormattedSchema() const;

    /// Return formatted content of TupleBuffer, contains timestamp if specified in config.
    std::string getFormattedBuffer(const Memory::TupleBuffer& inputBuffer);

    /// Reads a TupleBuffer and uses the supplied 'schema' to format it to CSV. Returns result as a string.
    static std::string tupleBufferToFormattedCSVString(Memory::TupleBuffer tbuffer, const SchemaPtr& schema);

    friend std::ostream& operator<<(std::ostream& out, const CSVFormat& format);

private:
    std::shared_ptr<Schema> schema;
    bool addTimestamp;
};

}

namespace fmt
{
template <>
struct formatter<NES::Sinks::CSVFormat> : ostream_formatter
{
};
}
