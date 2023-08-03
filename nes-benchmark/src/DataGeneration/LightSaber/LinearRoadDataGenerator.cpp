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
#include <DataGeneration/LightSaber/LinarRoadDataGenerator.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <fstream>
#include <iterator>
#include <utility>

namespace NES::Benchmark::DataGeneration {
LinearRoadDataGenerator::LinearRoadDataGenerator() : DataGenerator() {}

std::string LinearRoadDataGenerator::getName() { return "LinearRoad"; }

std::vector<Runtime::TupleBuffer> LinearRoadDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::vector<Runtime::TupleBuffer> buffers;
    buffers.reserve(numberOfBuffers);

    auto memoryLayout = getMemoryLayout(bufferSize);
    // read input file
    std::ifstream file(std::string(BENCHMARK_DATA_DIRECTORY) + "/lrb/lrb-data-small-ht.txt");
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

            if (line.empty()) {
                NES_THROW_RUNTIME_ERROR("Parsing Error: line was empty!");
            }

            std::istringstream iss(line);
            std::vector<std::string> words{std::istream_iterator<std::string>{iss}, std::istream_iterator<std::string>{}};
            dynamicBuffer[currentRecord]["creationTS"].write<int64_t>(std::stol(words[0]));
            dynamicBuffer[currentRecord]["vehicle"].write<int16_t>(std::stoi(words[1]));
            dynamicBuffer[currentRecord]["speed"].write<float>(std::stof(words[2]));
            dynamicBuffer[currentRecord]["highway"].write<int16_t>(std::stoi(words[3]));
            dynamicBuffer[currentRecord]["lane"].write<int16_t>(std::stoi(words[4]));
            dynamicBuffer[currentRecord]["direction"].write<int16_t>(std::stoi(words[5]));
            dynamicBuffer[currentRecord]["position"].write<int16_t>(std::stoi(words[6]));
        }
        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        buffers.emplace_back(buffer);
    }
    return buffers;
}
SchemaPtr LinearRoadDataGenerator::getSchema() {
    return Schema::create()
        ->addField("creationTS", BasicType::INT64)
        ->addField("vehicle", BasicType::INT16)
        ->addField("speed", BasicType::FLOAT32)
        ->addField("highway", BasicType::INT16)
        ->addField("lane", BasicType::INT16)
        ->addField("direction", BasicType::INT16)
        ->addField("position", BasicType::INT16);
}

std::string LinearRoadDataGenerator::toString() {
    std::ostringstream oss;
    oss << getName();
    return oss.str();
}

}// namespace NES::Benchmark::DataGeneration