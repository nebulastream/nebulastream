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

#include <DataProvider/DataProvider.hpp>
#include <DataProvider/ExternalProvider.hpp>
#include <DataProvider/InternalProvider.hpp>
#include <IngestionRateGeneration/IngestionRateGenerator.hpp>
#include <cstring>
#include <memory>

namespace NES::Benchmark::DataProvision {
DataProviderPtr DataProvider::createProvider(uint64_t providerId,
                                             NES::Benchmark::E2EBenchmarkConfigOverAllRuns& configOverAllRuns,
                                             std::vector<Runtime::TupleBuffer> buffers) {
    DataProviderMode dataProviderMode;
    if (configOverAllRuns.dataProviderMode->getValue() == "ZeroCopy") {
        dataProviderMode = DataProviderMode::ZERO_COPY;
    } else if (configOverAllRuns.dataProviderMode->getValue() == "MemCopy") {
        dataProviderMode = DataProviderMode::MEM_COPY;
    } else {
        NES_THROW_RUNTIME_ERROR("Could not parse dataProviderMode = " << configOverAllRuns.dataProviderMode->getValue() << "!");
    }

    if (configOverAllRuns.dataProvider->getValue() == "Internal") {
        return std::make_shared<InternalProvider>(providerId, dataProviderMode, buffers);
    } else if (configOverAllRuns.dataProvider->getValue() == "External") {
        auto ingestionRateGenerator =
            IngestionRateGeneration::IngestionRateGenerator::createIngestionRateGenerator(configOverAllRuns);
        return std::make_shared<ExternalProvider>(providerId, dataProviderMode, buffers, std::move(ingestionRateGenerator));
    } else {
        NES_THROW_RUNTIME_ERROR("Could not parse dataProvider = " << configOverAllRuns.dataProvider->getValue() << "!");
    }
}

DataProvider::DataProvider(uint64_t id, DataProvider::DataProviderMode providerMode) : id(id), providerMode(providerMode) {}

void DataProvider::provideNextBuffer(Runtime::TupleBuffer& buffer, uint64_t sourceId) {
    auto providedBuffer = readNextBuffer(sourceId);
    if (providedBuffer.has_value()) {
        switch (providerMode) {
            case DataProviderMode::ZERO_COPY: {
                auto dataPtr = reinterpret_cast<uintptr_t>(buffer.getBuffer());
                bool success = collector.insert(dataPtr, TupleBufferHolder(buffer));
                NES_ASSERT(success, "could not put buffer into collector");
                auto gcCallback = [dataPtr, this](Runtime::detail::MemorySegment*, Runtime::BufferRecycler*) {
                    NES_ASSERT(collector.erase(dataPtr), "Cannot recycler buffer");
                };
                // providedBuffer.value().addRecycleCallback(std::move(gcCallback));
                buffer = providedBuffer.value();
                return;
            };
            case DataProviderMode::MEM_COPY: {
                std::memcpy(buffer.getBuffer(), providedBuffer.value().getBuffer(), buffer.getBufferSize());
                providedBuffer.value().setCreationTimestampInMS(buffer.getCreationTimestampInMS());
                return;
            };
        }
    }
}

}// namespace NES::Benchmark::DataProvision
