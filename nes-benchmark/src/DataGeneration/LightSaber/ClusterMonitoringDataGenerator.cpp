#include <API/Schema.hpp>
#include <DataGenerators/LightSaber/ClusterMonitoringDataGenerator.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <fstream>
#include <iterator>
#include <utility>

namespace NES {

ClusterMonitoringDataGenerator::ClusterMonitoringDataGenerator(Runtime::BufferManagerPtr bufferManager)
    : DataGenerator(std::move(bufferManager)) {}

std::string ClusterMonitoringDataGenerator::getName() { return "ClusterMonitoring"; }

std::vector<Runtime::TupleBuffer> ClusterMonitoringDataGenerator::createData(uint64_t numberOfBuffers, uint64_t bufferSize) {
    std::vector<Runtime::TupleBuffer> buffers;
    auto memoryLayout = getMemoryLayout(bufferSize);
    // read input file
    std::string filePath = "resources/datasets/google-cluster-data/";
    std::ifstream file(std::string(RESOURCES_PATH) + "/google-cluster-data/google-cluster-data.txt");

    std::string line;
    std::vector<std::vector<std::string>> lines;
    while(std::getline(file, line)){
        std::istringstream iss(line);
        std::vector<std::string> words{std::istream_iterator<std::string>{iss}, std::istream_iterator<std::string>{}};
        lines.emplace_back(words);
    }

    uint64_t currentLineIndex = 0;
    for (uint64_t currentBuffer = 0; currentBuffer < numberOfBuffers; currentBuffer++) {
        auto buffer = allocateBuffer(bufferSize);
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t currentRecord = 0; currentRecord < dynamicBuffer.getCapacity(); currentRecord++) {
            // check if we reached the end of the file and start from the beginning
            if (currentLineIndex >= lines.size()) {
                currentLineIndex = 0;
            }

            auto words = lines[currentLineIndex];
            dynamicBuffer[currentRecord]["creationTS"].write<int64_t>(std::stol(words[0]));
            dynamicBuffer[currentRecord]["jobId"].write<int64_t>(std::stol(words[1]));
            dynamicBuffer[currentRecord]["taskId"].write<int64_t>(std::stol(words[2]));
            dynamicBuffer[currentRecord]["machineId"].write<int64_t>(std::stol(words[3]));
            dynamicBuffer[currentRecord]["eventType"].write<int16_t>((int16_t)std::stoi(words[4]));
            dynamicBuffer[currentRecord]["userId"].write<int16_t>((int16_t)std::stoi(words[5]));
            dynamicBuffer[currentRecord]["category"].write<int16_t>((int16_t)std::stoi(words[6]));
            dynamicBuffer[currentRecord]["priority"].write<int16_t>((int16_t)std::stoi(words[7]));
            dynamicBuffer[currentRecord]["cpu"].write<float>(std::stof(words[8]));
            dynamicBuffer[currentRecord]["ram"].write<float>(std::stof(words[9]));
            dynamicBuffer[currentRecord]["disk"].write<float>(std::stof(words[10]));
            dynamicBuffer[currentRecord]["constraints"].write<int16_t>((int16_t)std::stoi(words[11]));
            currentLineIndex++;
        }
        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        buffers.emplace_back(buffer);
    }
    return buffers;
}
SchemaPtr ClusterMonitoringDataGenerator::getSchema() {
    return Schema::create()
        ->addField("creationTS", INT64)
        ->addField("jobId", INT64)
        ->addField("taskId", INT64)
        ->addField("machineId", INT64)
        ->addField("eventType", INT16)
        ->addField("userId", INT16)
        ->addField("category", INT16)
        ->addField("priority", INT16)
        ->addField("cpu", FLOAT32)
        ->addField("ram", FLOAT32)
        ->addField("disk", FLOAT32)
        ->addField("constraints", INT16);
}

}// namespace NES