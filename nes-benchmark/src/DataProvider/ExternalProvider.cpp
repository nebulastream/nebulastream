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
                                   const DataProviderMode providerMode,
                                   const std::vector<Runtime::TupleBuffer> preAllocatedBuffers,
                                   IngestionRateGeneration::IngestionRateGeneratorPtr ingestionRateGenerator,
                                   bool throwException)
    : DataProvider(id, providerMode), preAllocatedBuffers(preAllocatedBuffers),
      ingestionRateGenerator(std::move(ingestionRateGenerator)), throwException(throwException) {
    predefinedIngestionRates = this->ingestionRateGenerator->generateIngestionRates();

    uint64_t maxIngestionRateValue = *(std::max_element(predefinedIngestionRates.begin(), predefinedIngestionRates.end()));
    bufferQueue = folly::MPMCQueue<TupleBufferHolder>(maxIngestionRateValue == 0 ? 1 : maxIngestionRateValue * 1000);
}

std::vector<Runtime::TupleBuffer>& ExternalProvider::getPreAllocatedBuffers() { return preAllocatedBuffers; }

folly::MPMCQueue<TupleBufferHolder>& ExternalProvider::getBufferQueue() { return bufferQueue; }

std::thread& ExternalProvider::getGeneratorThread() { return generatorThread; }

void ExternalProvider::start() {
    if (!started) {
        generatorThread = std::thread([this] {
            this->generateData();
        });
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

    std::unique_lock lock(mutexStartProvider);

    // calculate the number of buffers to produce per working time delta and append to the queue
    // note that the predefined ingestion rates represent the number of buffers to be produced per second,
    // so we multiply them with the working time delta and ingest the resulting number of buffers as quickly as possible into the system,
    // before we wait for the next period to start
    auto workingTimeDeltaInSec = workingTimeDeltaInMillSeconds / 1000.0;
    auto buffersToProducePerWorkingTimeDelta = predefinedIngestionRates[0] * workingTimeDeltaInSec;
    NES_ASSERT(buffersToProducePerWorkingTimeDelta > 0, "Ingestion rate is too small!");

    auto lastSecond = 0L;
    auto runningOverAllCount = 0L;
    auto ingestionRateIndex = 0L;

    started = true;

    // Manual unlocking is done before notifying, to avoid waking up the waiting thread only to block again
    // (see notify_one for details). Taken from https://en.cppreference.com/w/cpp/thread/condition_variable
    lock.unlock();
    cvStartProvider.notify_one();

    // continue producing buffers as long as the generator is running
    while (started) {
        // get the timestamp of the current period
        auto periodStartTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        auto buffersProcessedCount = 0;

        // produce the required number of buffers for the current period
        while (buffersProcessedCount < buffersToProducePerWorkingTimeDelta) {
            auto currentSecond =
                std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch()).count();

            // get the buffer to produce
            auto bufferIndex = runningOverAllCount++ % preAllocatedBuffers.size();
            auto buffer = preAllocatedBuffers[bufferIndex];

            // wrap the buffer in a tuple buffer and set its metadata
//            auto wrapBuffer = Runtime::TupleBuffer::wrapMemory(buffer.getBuffer(), buffer.getBufferSize(), this);
//            wrapBuffer.setNumberOfTuples(buffer.getNumberOfTuples());
//            wrapBuffer.setCreationTimestampInMS(
//                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
//                    .count());

            // create a buffer holder and write it to the queue
            TupleBufferHolder bufferHolder;
//            bufferHolder.bufferToHold = wrapBuffer;

            if (!bufferQueue.write(std::move(bufferHolder))) {
                NES_ERROR_OR_THROW_RUNTIME(throwException, "The queue is too small! This should not happen!");
            }

            // for the next second, recalculate the number of buffers to produce based on the next predefined ingestion rate
            if (lastSecond != currentSecond) {
                auto nextIndex = ingestionRateIndex % predefinedIngestionRates.size();
                buffersToProducePerWorkingTimeDelta = std::max(1.0, predefinedIngestionRates[nextIndex] * workingTimeDeltaInSec);

                lastSecond = currentSecond;
                ingestionRateIndex++;
            }

            buffersProcessedCount++;
        }

        // get the current time and wait until the next period start time
        auto currentTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        auto nextPeriodStartTime = periodStartTime + workingTimeDeltaInMillSeconds;

        if (nextPeriodStartTime < currentTime) {
            NES_ERROR_OR_THROW_RUNTIME(throwException, "The generator cannot produce data fast enough!");
        }

        while (currentTime < nextPeriodStartTime) {
            currentTime =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
                    .count();
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
        NES_WARNING("Buffer expected but not ready for now!");
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

bool ExternalProvider::isStarted() const { return started; }

void ExternalProvider::waitUntilStarted() {
    std::unique_lock lock(mutexStartProvider);
    cvStartProvider.wait(lock, [this] {
        return this->isStarted();
    });
}
void ExternalProvider::setThrowException(bool throwException) { this->throwException = throwException; }

}// namespace NES::Benchmark::DataProvision
