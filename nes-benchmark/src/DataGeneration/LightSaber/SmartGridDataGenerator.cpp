#include <API/Schema.hpp>
#include <DataGenerators/LightSaber/SmartGridDataGenerator.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <fstream>
#include <iterator>
#include <utility>

namespace NES {

SmartGridDataGenerator::SmartGridDataGenerator(Runtime::BufferManagerPtr bufferManager)
    : DataGenerator(std::move(bufferManager)) {}

std::string SmartGridDataGenerator::getName() { return "SmartGrid"; }

std::vector<Runtime::TupleBuffer> SmartGridDataGenerator::createData(uint64_t numberOfBuffers, uint64_t bufferSize) {
    std::vector<Runtime::TupleBuffer> buffers;
    auto memoryLayout = getMemoryLayout(bufferSize);
    // read input file
    std::ifstream file(std::string(RESOURCES_PATH) + "/smartgrid/smartgrid-data.txt");
    std::string line;
    std::vector<std::vector<std::string>> lines;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::vector<std::string> words{std::istream_iterator<std::string>{iss}, std::istream_iterator<std::string>{}};
        lines.emplace_back(words);
    }
    uint64_t linecounter = 0;
    for (uint64_t currentBuffer = 0; currentBuffer < numberOfBuffers; currentBuffer++) {
        auto buffer = allocateBuffer(bufferSize);
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
        ->addField("creationTS", INT64)
        ->addField("value", FLOAT32)
        ->addField("property", INT16)
        ->addField("plug", INT16)
        ->addField("household", INT16)
        ->addField("house", INT16);
}

}// namespace NES