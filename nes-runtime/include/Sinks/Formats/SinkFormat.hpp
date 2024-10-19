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
#include <fstream>
#include <optional>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Formats/FormatIterators/FormatIterator.hpp>
/**
 * @brief this class covers the different output formats that we offer in NES
 */
namespace NES
{

class SinkFormat
{
public:
    /**
     * @brief constructor for a sink format
     * @param schema
     * @param append
     */
    SinkFormat(SchemaPtr schema);

    /**
     * @brief constructor for a sink format
     * @param schema the schema
     * @param append flag to append or not
     * @param addTimestamp flag to add a timestamp in getFormattedBuffer
     */
    SinkFormat(SchemaPtr schema, bool addTimestamp);

    virtual ~SinkFormat() noexcept = default;

    /**
     * @brief Returns the schema of formatted according to the specific SinkFormat represented as string.
     * @return The formatted schema as string
     */
    virtual std::string getFormattedSchema() = 0;

    /**
    * @brief method to format a TupleBuffer
    * @param a tuple buffers pointer
    * @return formatted content of TupleBuffer
     */
    virtual std::string
    getFormattedBuffer(const Memory::TupleBuffer& inputBuffer, std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider)
        = 0;

    /**
    * @brief depending on the SinkFormat type, returns an iterator that can be used to retrieve tuples from the TupleBuffer
    * @param a tuple buffer pointer
    * @return TupleBuffer iterator
     */
    virtual FormatIterator getTupleIterator(Memory::TupleBuffer& inputBuffer) = 0;

    /**
     * @brief method to return the format as a string
     * @return format as string
     */
    virtual std::string toString() = 0;

    virtual FormatTypes getSinkFormat() = 0;

    SchemaPtr getSchemaPtr();
    void setSchemaPtr(SchemaPtr schema);

    std::shared_ptr<Memory::AbstractBufferProvider> getBufferManager();
    void setBufferManager(std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider);

    bool getAddTimestamp();
    void setAddTimestamp(bool addTimestamp);

protected:
    SchemaPtr schema;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    bool addTimestamp;
};

using SinkFormatPtr = std::shared_ptr<SinkFormat>;

}
