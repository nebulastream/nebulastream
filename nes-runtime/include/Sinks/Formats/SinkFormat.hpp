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

#ifndef NES_RUNTIME_INCLUDE_SINKS_FORMATS_SINKFORMAT_HPP_
#define NES_RUNTIME_INCLUDE_SINKS_FORMATS_SINKFORMAT_HPP_
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Formats/FormatIterators/FormatIterator.hpp>
#include <fstream>
#include <optional>
/**
 * @brief this class covers the different output formats that we offer in NES
 */
namespace NES {

class SinkFormat {
  public:
    /**
     * @brief constructor for a sink format
     * @param schema
     * @param append
     */
    SinkFormat(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager);

    /**
     * @brief constructor for a sink format
     * @param schema the schema
     * @param append flag to append or not
     * @param addTimestamp flag to add a timestamp in getFormattedBuffer
     */
    SinkFormat(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager, bool addTimestamp);

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
    virtual std::string getFormattedBuffer(Runtime::TupleBuffer& inputBuffer) = 0;

    /**
    * @brief depending on the SinkFormat type, returns an iterator that can be used to retrieve tuples from the TupleBuffer
    * @param a tuple buffer pointer
    * @return TupleBuffer iterator
     */
    virtual FormatIterator getTupleIterator(Runtime::TupleBuffer& inputBuffer) = 0;

    /**
     * @brief method to return the format as a string
     * @return format as string
     */
    virtual std::string toString() = 0;

    virtual FormatTypes getSinkFormat() = 0;

    SchemaPtr getSchemaPtr();
    void setSchemaPtr(SchemaPtr schema);

    Runtime::BufferManagerPtr getBufferManager();
    void setBufferManager(Runtime::BufferManagerPtr bufferManager);

    bool getAddTimestamp();
    void setAddTimestamp(bool addTimestamp);

  protected:
    SchemaPtr schema;
    Runtime::BufferManagerPtr bufferManager;
    bool addTimestamp;
};

using SinkFormatPtr = std::shared_ptr<SinkFormat>;

}// namespace NES
#endif  // NES_RUNTIME_INCLUDE_SINKS_FORMATS_SINKFORMAT_HPP_
