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
                                   IngestionRateGeneration::IngestionRateGeneratorPtr ingestionRateGenerator)
    : DataProvider(id, providerMode), preAllocatedBuffers(preAllocatedBuffers), ingestionRateGenerator(std::move(ingestionRateGenerator)) {
    predefinedIngestionRates = this->ingestionRateGenerator->generateIngestionRates();

    uint64_t maxIngestionRateValue = *(std::max_element(predefinedIngestionRates.begin(), predefinedIngestionRates.end()));
    bufferQueue = folly::MPMCQueue<TupleBufferHolder>(maxIngestionRateValue == 0 ? 1 : maxIngestionRateValue * 1000);
}

std::vector<Runtime::TupleBuffer>& ExternalProvider::getPreAllocatedBuffers() { return preAllocatedBuffers; }

folly::MPMCQueue<TupleBufferHolder>& ExternalProvider::getBufferQueue() { return bufferQueue; }

std::thread& ExternalProvider::getGeneratorThread() { return generatorThread; }

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
    auto workingTimeDeltaInSec = workingTimeDeltaInMillSeconds / 1000.0;
    auto buffersToProducePerWorkingTimeDelta = predefinedIngestionRates[0] * workingTimeDeltaInSec;
    NES_ASSERT(buffersToProducePerWorkingTimeDelta > 0, "Ingestion rate is too small!");

    auto lastSecond = 0L;
    auto runningOverAllCount = 0L;
    auto ingestionRateIndex = 0L;

    started = true;
    while (started) {
        auto periodStartTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        auto buffersProcessedCount = 0;

        while (buffersProcessedCount < buffersToProducePerWorkingTimeDelta) {
            auto currentSecond =
                std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch()).count();

            auto bufferIndex = runningOverAllCount++ % preAllocatedBuffers.size();
            auto buffer = preAllocatedBuffers[bufferIndex];

            auto wrapBuffer = Runtime::TupleBuffer::wrapMemory(buffer.getBuffer(), buffer.getBufferSize(), this);
            wrapBuffer.setNumberOfTuples(buffer.getNumberOfTuples());
            wrapBuffer.setCreationTimestamp(
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());

            TupleBufferHolder bufferHolder;
            bufferHolder.bufferToHold = wrapBuffer;

            if (!bufferQueue.write(std::move(bufferHolder))) {
                NES_THROW_RUNTIME_ERROR("The queue is too small! This should not happen!");
            }

            if (lastSecond != currentSecond) {
                if ((buffersToProducePerWorkingTimeDelta =
                         predefinedIngestionRates[ingestionRateIndex % predefinedIngestionRates.size()] * workingTimeDeltaInSec) == 0) {
                    buffersToProducePerWorkingTimeDelta = 1;
                }

                lastSecond = currentSecond;
                ingestionRateIndex++;
            }

            buffersProcessedCount++;
        }

        auto currentTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        auto nextPeriodStartTime = periodStartTime + workingTimeDeltaInMillSeconds;

        if (nextPeriodStartTime < currentTime) {
            NES_THROW_RUNTIME_ERROR("The generator cannot produce data fast enough!");
        }

        while (currentTime < nextPeriodStartTime) {
            currentTime =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        }
    }
}

std::optional<Runtime::TupleBuffer> ExternalProvider::readNextBuffer(uint64_t sourceId) {
    // For now, we only have a single source
    ((void) sourceId);

    TupleBufferHolder bufferHolder;
    auto res = false;

    while (started && !res) {
        res = bufferQueue.read(bufferHolder);
    }

    if (res) {
        return bufferHolder.bufferToHold;
    } else if (!started) {
        return std::nullopt;
    } else {
        NES_THROW_RUNTIME_ERROR("This should not happen! An empty buffer was returned while provider is started!");
    }
}

void ExternalProvider::recyclePooledBuffer(Runtime::detail::MemorySegment*) {}
void ExternalProvider::recycleUnpooledBuffer(Runtime::detail::MemorySegment*) {}

ExternalProvider::~ExternalProvider() {
    started = false;
    if (generatorThread.joinable()) {
        generatorThread.join();
    }

    preAllocatedBuffers.clear();
}
}// namespace NES::Benchmark::DateProviding
