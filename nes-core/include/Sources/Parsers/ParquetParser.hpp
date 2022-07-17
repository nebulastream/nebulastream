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

#ifndef NES_NES_CORE_INCLUDE_SOURCES_PARSERS_PARQUETPARSER_H_
#define NES_NES_CORE_INCLUDE_SOURCES_PARSERS_PARQUETPARSER_H_
#ifdef ENABLE_PARQUET_BUILD
#include "Runtime/MemoryLayout/DynamicTupleBuffer.hpp"
#include <API/Schema.hpp>
#include <Sources/Parsers/Parser.hpp>
#include <parquet/stream_reader.h>

namespace NES {
class ParquetParser;
using ParquetParserPtr = std::shared_ptr<ParquetParser>;

class ParquetParser : public Parser {
  public:
    ParquetParser(std::vector<PhysicalTypePtr> physical_types, parquet::StreamReader reader, SchemaPtr schema);

    bool writeInputTupleToTupleBuffer(const std::string& inputTuple,
                                      uint64_t tupleCount,
                                      Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer,
                                      const SchemaPtr& schema);

    bool writeToTupleBuffer(uint64_t tupleCount, Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer);


  private:
    parquet::StreamReader reader;
    SchemaPtr schema;


};
} // namepsace NES
#endif
#endif//NES_NES_CORE_INCLUDE_SOURCES_PARSERS_PARQUETPARSER_H_
