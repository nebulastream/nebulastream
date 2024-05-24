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
#include <DataGeneration/LightSaber/ClusterMonitoringDataGenerator.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <fstream>
#include <iterator>
#include <utility>

namespace NES::Benchmark::DataGeneration {

ClusterMonitoringDataGenerator::ClusterMonitoringDataGenerator() : DataGenerator() {}

std::string ClusterMonitoringDataGenerator::getName() { return "ClusterMonitoring"; }

std::vector<Runtime::TupleBuffer> ClusterMonitoringDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::vector<Runtime::TupleBuffer> buffers;
    buffers.reserve(numberOfBuffers);

    auto memoryLayout = getMemoryLayout(bufferSize);
    // read input file
    std::ifstream file(std::string(BENCHMARK_DATA_DIRECTORY) + "/cluster-monitoring/google-cluster-data.txt");

    std::string line;
    std::vector<std::vector<std::string>> lines;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::vector<std::string> words{std::istream_iterator<std::string>{iss}, std::istream_iterator<std::string>{}};
        lines.emplace_back(words);
    }

    uint64_t currentLineIndex = 0;
    for (uint64_t currentBuffer = 0; currentBuffer < numberOfBuffers; currentBuffer++) {
        auto buffer = allocateBuffer();
        auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
        for (uint64_t currentRecord = 0; currentRecord < testBuffer.getCapacity(); currentRecord++) {
            // check if we reached the end of the file and start from the beginning
            if (currentLineIndex >= lines.size()) {
                currentLineIndex = 0;
            }

            auto words = lines[currentLineIndex];
            testBuffer[currentRecord]["creationTS"].write<int64_t>(std::stol(words[0]));
            testBuffer[currentRecord]["jobId"].write<int64_t>(std::stol(words[1]));
            testBuffer[currentRecord]["taskId"].write<int64_t>(std::stol(words[2]));
            testBuffer[currentRecord]["machineId"].write<int64_t>(std::stol(words[3]));
            testBuffer[currentRecord]["eventType"].write<int16_t>((int16_t) std::stoi(words[4]));
            testBuffer[currentRecord]["userId"].write<int16_t>((int16_t) std::stoi(words[5]));
            testBuffer[currentRecord]["category"].write<int16_t>((int16_t) std::stoi(words[6]));
            testBuffer[currentRecord]["priority"].write<int16_t>((int16_t) std::stoi(words[7]));
            testBuffer[currentRecord]["cpu"].write<float>(std::stof(words[8]));
            testBuffer[currentRecord]["ram"].write<float>(std::stof(words[9]));
            testBuffer[currentRecord]["disk"].write<float>(std::stof(words[10]));
            testBuffer[currentRecord]["constraints"].write<int16_t>((int16_t) std::stoi(words[11]));
            currentLineIndex++;
        }
        testBuffer.setNumberOfTuples(testBuffer.getCapacity());
        buffers.emplace_back(buffer);
    }
    return buffers;
}

SchemaPtr ClusterMonitoringDataGenerator::getSchema() {
    return Schema::create()
        ->addField("creationTS", BasicType::INT64)
        ->addField("jobId", BasicType::INT64)
        ->addField("taskId", BasicType::INT64)
        ->addField("machineId", BasicType::INT64)
        ->addField("eventType", BasicType::INT16)
        ->addField("userId", BasicType::INT16)
        ->addField("category", BasicType::INT16)
        ->addField("priority", BasicType::INT16)
        ->addField("cpu", BasicType::FLOAT32)
        ->addField("ram", BasicType::FLOAT32)
        ->addField("disk", BasicType::FLOAT32)
        ->addField("constraints", BasicType::INT16);
}

Configurations::SchemaTypePtr ClusterMonitoringDataGenerator::getSchemaType() {
    const char* dataTypeI64 = "INT64";
    const char* dataTypeI16 = "INT16";
    const char* dataTypeF32 = "FLOAT32";
    std::vector<Configurations::SchemaFieldDetail> schemaFieldDetails;
    schemaFieldDetails.emplace_back("creationTS", dataTypeI64);
    schemaFieldDetails.emplace_back("jobId", dataTypeI64);
    schemaFieldDetails.emplace_back("taskId", dataTypeI64);
    schemaFieldDetails.emplace_back("machineId", dataTypeI64);
    schemaFieldDetails.emplace_back("eventType", dataTypeI16);
    schemaFieldDetails.emplace_back("userId", dataTypeI16);
    schemaFieldDetails.emplace_back("category", dataTypeI16);
    schemaFieldDetails.emplace_back("priority", dataTypeI16);
    schemaFieldDetails.emplace_back("cpu", dataTypeF32);
    schemaFieldDetails.emplace_back("ram", dataTypeF32);
    schemaFieldDetails.emplace_back("disk", dataTypeF32);
    schemaFieldDetails.emplace_back("constraints", dataTypeI16);
    return Configurations::SchemaType::create(schemaFieldDetails);
}

std::string ClusterMonitoringDataGenerator::toString() {
    std::ostringstream oss;
    oss << getName();
    return oss.str();
}
}// namespace NES::Benchmark::DataGeneration
