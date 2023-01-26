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

#include <DataProvider/ExternalProvider.hpp>

namespace NES::Benchmark::DataProvision {
ExternalProvider::ExternalProvider(uint64_t id,
                                   DataProviderMode providerMode,
                                   std::vector<Runtime::TupleBuffer> preAllocatedBuffers,
                                   IngestionRateGeneration::IngestionRateGenerator ingestionRateGenerator)
    : DataProvider(id, providerMode), preAllocatedBuffers(preAllocatedBuffers), ingestionRateGenerator(ingestionRateGenerator) {}

void ExternalProvider::start() {
    if (!started) {
        generatorThread = std::thread([this] {this->generateData();});
    }
}

void ExternalProvider::stop() {
    started = false;
    if (generatorThread.joinable()) {
        generatorThread.join();
    }

    preAllocatedBuffers.clear();
}

void ExternalProvider::generateData() {
    sleep(5);   // TODO why is this sleep necessary?

    auto predefinedIngestionRates = ingestionRateGenerator.generateIngestionRates();

    uint64_t maxIngestionRateValue = *(std::max_element(predefinedIngestionRates.begin(), predefinedIngestionRates.end()));
    bufferQueue = folly::MPMCQueue<TupleBufferHolder>(maxIngestionRateValue == 0 ? 1 : maxIngestionRateValue * 1000);

    uint64_t nextPeriodStartTime;
    uint64_t periodStartTime;
    uint64_t periodEndTime;
    uint64_t currentTime;
    uint64_t currentSecond;
    uint64_t lastSecond = 0;

    uint64_t buffersProcessedCount;
    // TODO why is workingTimeDelta not being used here?
    uint64_t buffersToProducePer10ms = predefinedIngestionRates[0];
    NES_ASSERT(buffersToProducePer10ms != 0, "Ingestion ratio is too small");

    started = true;

    uint64_t runningOverAllCount = 0;
    uint64_t ingestionRateIndex = 0;

    while (started) {
        periodStartTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        buffersProcessedCount = 0;

        while (buffersProcessedCount < buffersToProducePer10ms) {
            currentSecond =
                std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch()).count();

            auto bufferIndex = runningOverAllCount++ % preAllocatedBuffers.size();
            auto buffer = preAllocatedBuffers[bufferIndex];

            auto wrapBuffer = Runtime::TupleBuffer::wrapMemory(buffer.getBuffer(), buffer.getBufferSize(), this);
            wrapBuffer.setNumberOfTuples(buffer.getNumberOfTuples());
            wrapBuffer.setCreationTimestamp(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());

            TupleBufferHolder bufferHolder;
            bufferHolder.bufferToHold = wrapBuffer;

            if (!bufferQueue.write(std::move(bufferHolder))) {
                NES_THROW_RUNTIME_ERROR("The queue is too small! This should not happen!");
            }

            if (lastSecond != currentSecond) {
                // TODO change /100 according to workingTimeDelta (see ExternalProvider.hpp)
                if ((buffersToProducePer10ms = predefinedIngestionRates[ingestionRateIndex % predefinedIngestionRates.size()] / 100) == 0) {
                    buffersToProducePer10ms = 1;
                }

                lastSecond = currentSecond;
                ingestionRateIndex++;
            }

            buffersProcessedCount++;
        }// while (buffersProcessedCount < buffersToProducePer10ms)

        periodEndTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        nextPeriodStartTime = periodStartTime + 10; // TODO change +10 according to workingTimeDelta (see ExternalProvider.hpp)

        if (nextPeriodStartTime < periodEndTime) {
            NES_THROW_RUNTIME_ERROR("The generator cannot produce data fast enough!");
        }

        // TODO maybe just use periodEndTime
        currentTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();

        while (currentTime < nextPeriodStartTime) {
            currentTime =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        }
    }// while (started)
}
}// namespace NES::Benchmark::DateProviding
