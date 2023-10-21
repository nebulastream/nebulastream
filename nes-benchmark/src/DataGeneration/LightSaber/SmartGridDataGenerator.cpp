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
#include <API/Schema.hpp>
#include <Configurations/Coordinator/SchemaType.hpp>
#include <DataGeneration/LightSaber/SmartGridDataGenerator.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <fstream>
#include <iterator>
#include <utility>

namespace NES::Benchmark::DataGeneration {
SmartGridDataGenerator::SmartGridDataGenerator() : DataGenerator() {}

std::string SmartGridDataGenerator::getName() { return "SmartGrid"; }

std::vector<Runtime::TupleBuffer> SmartGridDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::vector<Runtime::TupleBuffer> buffers;
    buffers.reserve(numberOfBuffers);

    auto memoryLayout = getMemoryLayout(bufferSize);
    // read input file
    std::ifstream file(std::string(BENCHMARK_DATA_DIRECTORY) + "/smartgrid/smartgrid-data.txt");
    std::string line;
    std::vector<std::vector<std::string>> lines;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::vector<std::string> words{std::istream_iterator<std::string>{iss}, std::istream_iterator<std::string>{}};
        lines.emplace_back(words);
    }
    uint64_t linecounter = 0;
    for (uint64_t currentBuffer = 0; currentBuffer < numberOfBuffers; currentBuffer++) {
        auto buffer = allocateBuffer();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t currentRecord = 0; currentRecord < dynamicBuffer.getCapacity(); currentRecord++) {
            // check if we reached the end of the file and start from the beginning
            if (linecounter == lines.size()) {
                linecounter = 0;
            }

            std::vector<std::string> words = lines[linecounter];

            dynamicBuffer[currentRecord]["creationTS"].write<int64_t>(std::stol(words[0]));
            dynamicBuffer[currentRecord]["value"].write<float>(std::stof(words[1]));
            dynamicBuffer[currentRecord]["property"].write<int16_t>(std::stoi(words[2]));
            dynamicBuffer[currentRecord]["plug"].write<int16_t>(std::stoi(words[3]));
            dynamicBuffer[currentRecord]["household"].write<int16_t>(std::stoi(words[4]));
            dynamicBuffer[currentRecord]["house"].write<int16_t>(std::stoi(words[5]));
            linecounter++;
        }
        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        buffers.emplace_back(buffer);
    }
    return buffers;
}
SchemaPtr SmartGridDataGenerator::getSchema() {
    return Schema::create()
        ->addField("creationTS", BasicType::INT64)
        ->addField("value", BasicType::FLOAT32)
        ->addField("property", BasicType::INT16)
        ->addField("plug", BasicType::INT16)
        ->addField("household", BasicType::INT16)
        ->addField("house", BasicType::INT16);
}

Configurations::SchemaTypePtr SmartGridDataGenerator::getSchemaType() {
    const char* length = "0";
    const char* dataTypeI64 = "INT64";
    const char* dataTypeF32 = "FLOAT32";
    const char* dataTypeI16 = "INT16";
    std::vector<Configurations::SchemaFieldDetail> schemaFiledDetails;
    schemaFiledDetails.emplace_back("creationTS", dataTypeI64, length);
    schemaFiledDetails.emplace_back("value", dataTypeF32, length);
    schemaFiledDetails.emplace_back("property", dataTypeI16, length);
    schemaFiledDetails.emplace_back("plug", dataTypeI16, length);
    schemaFiledDetails.emplace_back("household", dataTypeI16, length);
    schemaFiledDetails.emplace_back("house", dataTypeI16, length);
    return Configurations::SchemaType::create(schemaFiledDetails);
}

std::string SmartGridDataGenerator::toString() {
    std::ostringstream oss;
    oss << getName();
    return oss.str();
}
}// namespace NES::Benchmark::DataGeneration