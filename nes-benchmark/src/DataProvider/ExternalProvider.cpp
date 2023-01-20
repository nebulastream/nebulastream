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

namespace NES::Benchmark::DataProviding {
ExternalProvider::ExternalProvider(uint64_t id,
                                   std::vector<Runtime::TupleBuffer> preAllocatedBuffers,
                                   uint64_t ingestionRateInBuffers,
                                   uint64_t experimentRuntime, // TODO maybe change variable name
                                   DataProviderMode providerMode,
                                   std::string ingestionFrequencyDistribution)
    : DataProvider(id, providerMode), preAllocatedBuffers(preAllocatedBuffers), ingestionRateInBuffers(ingestionRateInBuffers),
      experimentRuntime(experimentRuntime), ingestionFrequencyDistribution(getDistributionFromString(ingestionFrequencyDistribution)) {

    generateIngestionRates();
    uint64_t maxIngestionRateValue = *(std::max_element(predefinedIngestionRates.begin(), predefinedIngestionRates.end()));
    bufferQueue = folly::MPMCQueue<TupleBufferHolder>(maxIngestionRateValue == 0 ? 1 : maxIngestionRateValue * 1000);
}

IngestionFrequencyDistribution ExternalProvider::getDistributionFromString(std::string ingestionDistribution) {
    if (ingestionDistribution == "UNIFORM") return UNIFORM;
    else if (ingestionDistribution == "SINUS") return SINUS;
    else if (ingestionDistribution == "COSINUS") return COSINUS;
    else if (ingestionDistribution == "M1") return M1;
    else if (ingestionDistribution == "M2") return M2;
    else if (ingestionDistribution == "D1") return D1;
    else if (ingestionDistribution == "D2") return D2;
    else NES_THROW_RUNTIME_ERROR("Provider mode not supported");
}

void ExternalProvider::generateIngestionRates() {
    for (uint64_t i = 0; i < experimentRuntime; i++) {
        if (ingestionFrequencyDistribution == UNIFORM) {
            predefinedIngestionRates.push_back(ingestionRateInBuffers);
        } else if (ingestionFrequencyDistribution == SINUS || ingestionFrequencyDistribution == COSINUS) {
            generateTrigonometricValues(i);
        }
        // TODO replace %18 and %30 with %(vector.size) when M1 through D2 are implemented as a vector
        else if (ingestionFrequencyDistribution == M1) {
            predefinedIngestionRates.push_back(m1Values[i % 18]);
        } else if (ingestionFrequencyDistribution == M2) {
            predefinedIngestionRates.push_back(m2Values[i % 18]);
        } else if (ingestionFrequencyDistribution == D1) {
            predefinedIngestionRates.push_back(d1Values[i % 30]);
        } else if (ingestionFrequencyDistribution == D2) {
            predefinedIngestionRates.push_back(d2Values[i % 30]);
        }
    }
}

void ExternalProvider::generateTrigonometricValues(uint64_t xValue) {
    if (ingestionFrequencyDistribution == SINUS) {
        // this is a sinus function with period length of experimentRuntime
        auto value = (long double)(0.5 * (1 + sin(2 * (long double)std::numbers::pi * xValue / experimentRuntime))) * ingestionRateInBuffers;
        predefinedIngestionRates.push_back(value);
    } else {
        // this is a cosinus function with period length of experimentRuntime
        auto value = (long double)(0.5 * (1 + cos(2 * (long double)std::numbers::pi * xValue / experimentRuntime))) * ingestionRateInBuffers;
        predefinedIngestionRates.push_back(value);
    }
}

void ExternalProvider::start() {
    if (!started) {
        generatorThread = std::thread([this] {this->generateData();});
        started = true;
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
                if ((buffersToProducePer10ms = predefinedIngestionRates[ingestionRateIndex % experimentRuntime] / 100) == 0) {
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
