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
#ifdef ENABLE_PARQUET_BUILD
#include <Sources/Parsers/ParquetParser.hpp>
//#include <Sources/ParquetStaticVariables.hpp>
namespace NES {
ParquetParser::ParquetParser(std::vector<PhysicalTypePtr> physical_types,
                                  parquet::StreamReader reader,
                                  SchemaPtr schema)
    : Parser(physical_types), reader(std::move(reader)), schema(std::move(schema)) {}

bool ParquetParser::writeInputTupleToTupleBuffer([[maybe_unused]] const std::string& inputTuple,
                                                 [[maybe_unused]] uint64_t tupleCount,
                                                 [[maybe_unused]] Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer,
                                                 [[maybe_unused]] const SchemaPtr& schema) {
    NES_DEBUG("Function not applicable for ParquetParser");
    return false;
}
bool ParquetParser::writeToTupleBuffer(uint64_t tupleCount, Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer) {
    if(reader.eof()){
        NES_DEBUG("Parquet File end");
        return false;
    }
    uint16_t uint16;
    double doubleType;
    std:: string string;
    bool boolean;
    uint32_t uint32;
    reader >> uint16 >> doubleType >> string >> boolean >> uint32 >> parquet::EndRow;//hardcoded types
    tupleBuffer[tupleCount][0].write<uint16_t>(uint16);
    auto data1 = tupleBuffer[tupleCount][0].read<uint16_t>();
    tupleBuffer[tupleCount][1].write<double>(doubleType);
    auto data2 = tupleBuffer[tupleCount][1].read<double>();
    char* value = tupleBuffer[tupleCount][2].read<char*>();
    strcpy(value,string.c_str());
    auto data3 = tupleBuffer[tupleCount][2].read<char*>();
    tupleBuffer[tupleCount][3].write<bool>(boolean);
    auto data4 = tupleBuffer[tupleCount][3].read<bool>();
    tupleBuffer[tupleCount][4].write<uint32_t>(uint32);
    auto data5 = tupleBuffer[tupleCount][4].read<uint32_t>();
    //NES_DEBUG(tupleBuffer.toString(schema));
    return true;
}

} //namespace NES
#endif

