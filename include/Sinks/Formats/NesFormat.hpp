/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_SINKS_FORMATS_NESFORMAT_HPP_
#define NES_INCLUDE_SINKS_FORMATS_NESFORMAT_HPP_

#include <Sinks/Formats/SinkFormat.hpp>
namespace NES {

class SerializableSchema;

class NesFormat : public SinkFormat {
  public:
    NesFormat(SchemaPtr schema, BufferManagerPtr bufferManager);

    /**
    * @brief method to write a TupleBuffer
    * @param a tuple buffers pointer
    * @return vector of Tuple buffer containing the content of the tuplebuffer
     */
    std::vector<TupleBuffer> getData(TupleBuffer& inputBuffer);

    /**
    * @brief method to write the schema of the data
    * @return TupleBuffer containing the schema
    */
    std::optional<TupleBuffer> getSchema();

    /**
   * @brief method to return the format as a string
   * @return format as string
   */
    std::string toString();

    /**
     * @brief return sink format
     * @return sink format
     */
    SinkFormatTypes getSinkFormat();

  private:
    SerializableSchema* serializedSchema;
};
}// namespace NES
#endif//NES_INCLUDE_SINKS_FORMATS_NESFORMAT_HPP_
