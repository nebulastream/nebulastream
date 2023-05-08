#include <API/Schema.hpp>
#include <DataGenerators/LightSaber/LinarRoadDataGenerator.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <fstream>
#include <iterator>
#include <utility>

namespace NES {

LinearRoadDataGenerator::LinearRoadDataGenerator(Runtime::BufferManagerPtr bufferManager)
    : DataGenerator(std::move(bufferManager)) {}

std::string LinearRoadDataGenerator::getName() { return "LinearRoad"; }

std::vector<Runtime::TupleBuffer> LinearRoadDataGenerator::createData(uint64_t numberOfBuffers, uint64_t bufferSize) {
    std::vector<Runtime::TupleBuffer> buffers;
    auto memoryLayout = getMemoryLayout(bufferSize);
    // read input file
    std::ifstream file(std::string(RESOURCES_PATH) + "/lrb/lrb-data-small-ht.txt");
    std::string line;

    for (uint64_t currentBuffer = 0; currentBuffer < numberOfBuffers; currentBuffer++) {
        auto buffer = allocateBuffer(bufferSize);
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
        ->addField("creationTS", INT64)
        ->addField("vehicle", INT16)
        ->addField("speed", FLOAT32)
        ->addField("highway", INT16)
        ->addField("lane", INT16)
        ->addField("direction", INT16)
        ->addField("position", INT16);
}

}// namespace NES