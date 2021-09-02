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

#ifndef NES_INCLUDE_SOURCES_PARSERS_CSVPARSER_HPP_
#define NES_INCLUDE_SOURCES_PARSERS_CSVPARSER_HPP_

#include <Sources/Parsers/Parser.hpp>

namespace NES {

class CSVParser : public Parser {

 public:
  /**
   * @brief public constructor for CSV input data parser
   */
  CSVParser(uint64_t tupleSize, uint64_t numberOfSchemaFields, std::vector<NES::PhysicalTypePtr> physicalTypes,
            std::string const& delimiter);

  /**
   * @brief takes csv string line as input, casts its values to the correct types and writes it to the TupleBuffer
   * @param csvInput: csv string value(one line) that is cast to the PhysicalType and written to the TupleBuffer
   * @param tupleCount: the number of tuples already written to the current TupleBuffer
   * @param buffer: the TupleBuffer to which the value is written
   */
  void writeInputTupleToTupleBuffer(std::string csvInput, uint64_t tupleCount, NES::Runtime::TupleBuffer& tupleBuffer) override;

 private:
  uint64_t tupleSize;
  uint64_t numberOfSchemaFields;
  std::vector<NES::PhysicalTypePtr> physicalTypes;
  std::string delimiter;
};
#endif //NES_INCLUDE_SOURCES_PARSERS_CSVPARSER_HPP_
}// namespace NES