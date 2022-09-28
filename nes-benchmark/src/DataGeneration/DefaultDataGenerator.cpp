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

#include <DataGeneration/DefaultDataGenerator.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <random>

namespace NES::DataGeneration {
    DefaultDataGenerator::DefaultDataGenerator(Runtime::BufferManagerPtr bufferManager,
                                               uint64_t minValue,
                                               uint64_t maxValue)  : DataGenerator(std::move(bufferManager)),
                                                                     minValue(minValue), maxValue(maxValue) {}

    std::vector<Runtime::TupleBuffer> DefaultDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numberOfBuffers);

        auto memoryLayout = this->getMemoryLayout(bufferSize);
        NES_INFO("Default source mode");



        uint64_t noTuplesInOnePercent = (numberOfBuffers) / 100;
        for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
            auto buffer = allocateBuffer(bufferSize);
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
            if (curBuffer % noTuplesInOnePercent == 0) {
                NES_INFO("DefaultDataGenerator: currently at " << (((double)curBuffer / numberOfBuffers) * 100) << "%");
            }

            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> distribution(minValue, maxValue);

            for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity(); ++curRecord) {
                auto value = distribution(gen);
                dynamicBuffer[curRecord]["id"].write<uint64_t>(curRecord);
                dynamicBuffer[curRecord]["value"].write<uint64_t>(value);
                dynamicBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
                dynamicBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
            }

            dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
            createdBuffers[curBuffer] = buffer;
        }


        NES_INFO("Created all buffers!");

        return createdBuffers;
    }

    NES::SchemaPtr DefaultDataGenerator::getSchema() {
        return Schema::create()->addField(createField("id", NES::UINT64))
                               ->addField(createField("value", NES::UINT64))
                               ->addField(createField("payload", NES::UINT64))
                               ->addField(createField("timestamp", NES::UINT64));
    }

    std::string DefaultDataGenerator::getName() {
        return "Default";
    }

} // namespace NES::DataGeneration