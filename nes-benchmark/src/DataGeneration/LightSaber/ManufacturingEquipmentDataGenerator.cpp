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
#include <DataGeneration/LightSaber/ManufacturingEquipmentDataGenerator.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <fstream>
#include <iterator>
#include <utility>

namespace NES::Benchmark::DataGeneration {
ManufacturingEquipmentDataGenerator::ManufacturingEquipmentDataGenerator() : DataGenerator() {}

std::string ManufacturingEquipmentDataGenerator::getName() { return "ManufacturingEquipment"; }

std::vector<Runtime::TupleBuffer> ManufacturingEquipmentDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::vector<Runtime::TupleBuffer> buffers;
    buffers.reserve(numberOfBuffers);

    auto memoryLayout = getMemoryLayout(bufferSize);
    // read input file
    std::ifstream file(std::string(BENCHMARK_DATA_DIRECTORY) + "/manufacturing-equipment/DEBS2012-small.txt");
    std::string line;

    for (uint64_t currentBuffer = 0; currentBuffer < numberOfBuffers; currentBuffer++) {
        auto buffer = allocateBuffer();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t currentRecord = 0; currentRecord < dynamicBuffer.getCapacity(); currentRecord++) {
            // check if we reached the end of the file and start from the beginning
            if (!std::getline(file, line)) {
                file.seekg(0);
                std::getline(file, line);
            }

            std::istringstream iss(line);
            std::vector<std::string> words{std::istream_iterator<std::string>{iss}, std::istream_iterator<std::string>{}};
            dynamicBuffer[currentRecord]["creationTS"].write<int64_t>(std::stol(words[0]));
            dynamicBuffer[currentRecord]["messageIndex"].write<int64_t>(std::stol(words[1]));
            dynamicBuffer[currentRecord]["mf01"].write<int16_t>(std::stol(words[2]));
            dynamicBuffer[currentRecord]["mf02"].write<int16_t>(std::stol(words[3]));
            dynamicBuffer[currentRecord]["mf03"].write<int16_t>(std::stoi(words[4]));
            dynamicBuffer[currentRecord]["pc13"].write<int16_t>(std::stoi(words[5]));
            dynamicBuffer[currentRecord]["pc14"].write<int16_t>(std::stoi(words[6]));
            dynamicBuffer[currentRecord]["pc15"].write<int16_t>(std::stoi(words[7]));
            dynamicBuffer[currentRecord]["pc25"].write<uint16_t>(std::stoi(words[8]));
            dynamicBuffer[currentRecord]["pc26"].write<uint16_t>(std::stoi(words[9]));
            dynamicBuffer[currentRecord]["pc27"].write<uint16_t>(std::stoi(words[10]));
            dynamicBuffer[currentRecord]["res"].write<uint16_t>(std::stoi(words[11]));
            dynamicBuffer[currentRecord]["bm05"].write<int16_t>(std::stoi(words[12]));
            dynamicBuffer[currentRecord]["bm06"].write<int16_t>(std::stoi(words[13]));
        }
        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        buffers.emplace_back(buffer);
    }
    return buffers;
}
SchemaPtr ManufacturingEquipmentDataGenerator::getSchema() {
    return Schema::create()
        ->addField("creationTS", BasicType::INT64)
        ->addField("messageIndex", BasicType::INT64)
        ->addField("mf01", BasicType::INT16)
        ->addField("mf02", BasicType::INT16)
        ->addField("mf03", BasicType::INT16)
        ->addField("pc13", BasicType::INT16)
        ->addField("pc14", BasicType::INT16)
        ->addField("pc15", BasicType::INT16)
        ->addField("pc25", BasicType::UINT16)
        ->addField("pc26", BasicType::UINT16)
        ->addField("pc27", BasicType::UINT16)
        ->addField("res", BasicType::UINT16)
        ->addField("bm05", BasicType::INT16)
        ->addField("bm06", BasicType::INT16);
}

std::string ManufacturingEquipmentDataGenerator::toString() {
    std::ostringstream oss;
    oss << getName();
    return oss.str();
}

}// namespace NES::Benchmark::DataGeneration