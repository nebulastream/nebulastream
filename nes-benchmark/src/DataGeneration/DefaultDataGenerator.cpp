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
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Benchmark::DataGeneration {
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

            Runtime::TupleBuffer bufferRef = allocateBuffer();
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferRef);

            if (memoryLayout->getSchema()->getLayoutType() == Schema::ROW_LAYOUT) {
                auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(memoryLayout->getSchema(), bufferSize);
                auto rowLayoutBuffer = rowLayout->bind(bufferRef);

                for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity(); ++curRecord) {
                    auto value = (curRecord % (maxValue - minValue)) + minValue;
                    rowLayoutBuffer->pushRecord<false>(std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>(curRecord,
                                                                                                          value * 0,
                                                                                                          curRecord,
                                                                                                          curRecord));
                }

            } else {
                for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity(); ++curRecord) {
                    auto value = (curRecord % (maxValue - minValue)) + minValue;
                    dynamicBuffer[curRecord]["id"].write<uint64_t>(curRecord);
                    dynamicBuffer[curRecord]["value"].write<uint64_t>(value);
                    dynamicBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
                    dynamicBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
                }
            }

            if (curBuffer % noTuplesInOnePercent == 0) {
                NES_INFO("DefaultDataGenerator: currently at " << (((double)curBuffer / numberOfBuffers) * 100) << "%");
            }

            dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
            createdBuffers.emplace_back(bufferRef);
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