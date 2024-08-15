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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_PARSERS_CSVPARSER_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_PARSERS_CSVPARSER_HPP_

#include <Sources/Parsers/Parser.hpp>
#include <Util/TestTupleBuffer.hpp>

namespace NES
{

class CSVParser : public Parser
{
public:
    CSVParser(uint64_t numberOfSchemaFields, std::vector<NES::PhysicalTypePtr> physicalTypes, std::string delimiter);

    /// takes csv string line as input, casts its values to the correct types and writes it to the TupleBuffer
    bool writeInputTupleToTupleBuffer(
        std::string_view csvInput,
        uint64_t tupleCount,
        Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer,
        const SchemaPtr& schema,
        const Runtime::BufferManagerPtr& bufferManager) override;

private:
    uint64_t numberOfSchemaFields;
    std::vector<NES::PhysicalTypePtr> physicalTypes;
    std::string delimiter;
};

} /// namespace NES
#endif /// NES_RUNTIME_INCLUDE_SOURCES_PARSERS_CSVPARSER_HPP_
