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

#ifndef NES_CORE_INCLUDE_SINKS_FORMATS_ARROWFORMAT_HPP_
#define NES_CORE_INCLUDE_SINKS_FORMATS_ARROWFORMAT_HPP_

#include <Sinks/Formats/SinkFormat.hpp>
#include <arrow/io/type_fwd.h>
#include <arrow/ipc/type_fwd.h>
#include <arrow/type_fwd.h>

namespace NES {

class ArrowFormat : public SinkFormat {
  public:
    ArrowFormat(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager);
    virtual ~ArrowFormat() noexcept = default;

    /**
     * @brief Returns the schema of formatted according to the specific SinkFormat represented as string.
     * @return The formatted schema as string
     */
    std::string getFormattedSchema() override;

    /**
    * @brief method to format a TupleBuffer
    * @param a reference to input TupleBuffer
    * @return Formatted content of tuple buffer
     */
    std::string getFormattedBuffer(Runtime::TupleBuffer& inputBuffer) override;

    /**
    * @brief method to write a TupleBuffer
    * @param a reference to input TupleBuffer
    * @return vector of Tuple buffer containing the content of the tuple buffer
     */
    FormatIterator getTupleIterator(Runtime::TupleBuffer& inputBuffer) override;

    /**
    * @brief method to get the schema from the arrow format
    * @return return the arrow schema
    */
    std::shared_ptr<arrow::Schema> getArrowSchema();

    /**
    * @brief method to get the arrow arrays from tuple buffer
    * @param a reference to input TupleBuffer
    * @return a vector of Arrow Arrays
    */
    std::vector<std::shared_ptr<arrow::Array>> getArrowArrays(Runtime::TupleBuffer& inputBuffer);

    /**
     * @brief method to return the format as a string
     * @return format as string
     */
    std::string toString() override;

    /**
     * @brief return sink format
     * @return sink format
     */
    FormatTypes getSinkFormat() override;

  private:
    /**
    * @brief method that creates arrow arrays based on the schema
    * @return a vector of empty arrow arrays
    */
    std::vector<std::shared_ptr<arrow::Array>> buildArrowArrays();
};
}// namespace NES
#endif// NES_CORE_INCLUDE_SINKS_FORMATS_ARROWFORMAT_HPP_
