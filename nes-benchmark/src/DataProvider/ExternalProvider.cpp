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
#include <Util/UtilityFunctions.hpp>

namespace NES::Benchmark::DataProviding {
ExternalProvider::ExternalProvider(uint64_t id,
                                   std::vector<Runtime::TupleBuffer> preAllocatedBuffers,
                                   uint64_t ingestionRateInBuffers, // TODO evtl. anderer Name?
                                   uint64_t experimentRuntime, // TODO anderen Namen überlegen
                                   DataProviderMode providerMode,
                                   std::string ingestionFrequencyDistribution)
    : DataProvider(id, providerMode), preAllocatedBuffers(preAllocatedBuffers), ingestionRateInBuffers(ingestionRateInBuffers),
      ingestionFrequencyDistribution(getModeFromString(ingestionFrequencyDistribution)) {

    // TODO set predefinedIngestionRates for SINUS and COSINUS
    uint64_t ingestionRateValue = 0;
    if (this->ingestionFrequencyDistribution != UNIFORM && this->ingestionFrequencyDistribution != SINUS && this->ingestionFrequencyDistribution != COSINUS) {
        generateFrequencies(experimentRuntime);
        auto maxFrequency = std::max_element(predefinedIngestionRates.begin(), predefinedIngestionRates.end());
        ingestionRateValue = *maxFrequency;
    } else {
        ingestionRateValue = ingestionRateInBuffers;
    }

    bufferQueue = folly::MPMCQueue<TupleBufferHolder>(ingestionRateValue == 0 ? 1 : ingestionRateValue * 1000);
}

IngestionFrequencyDistribution ExternalProvider::getModeFromString(std::string providerMode) {
    if (providerMode == "UNIFORM") return UNIFORM;
    else if (providerMode == "SINUS") return SINUS;
    else if (providerMode == "COSINUS") return COSINUS;
    else if (providerMode == "M1") return M1;
    else if (providerMode == "M2") return M2;
    else if (providerMode == "D1") return D1;
    else if (providerMode == "D2") return D2;
    else NES_THROW_RUNTIME_ERROR("Provider mode not supported");
}

void ExternalProvider::generateFrequencies(uint64_t experimentRuntime) {
    //const std::string delim = ",";

    std::string m1ValueString = "35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,"
                                "35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000";
    std::string m2ValueString = "10000,20000,30000,40000,50000,60000,70000,80000,90000,100000,90000,80000,70000,60000,50000,40000,30000,20000,"
                                "10000,20000,30000,40000,50000,60000,70000,80000,90000,100000,90000,80000,70000,60000,50000,40000,30000,20000,10000";

    std::string d1ValueString = "10000,20000,30000,40000,50000,60000,70000,80000,90000,100000,90000,80000,70000,60000,50000,"
                                "40000,30000,20000,30000,40000,50000,60000,70000,80000,90000,100000,90000,80000,70000,60000";
    std::string d2ValueString = "100000,90000,80000,70000,60000,50000,40000,30000,20000,10000,20000,30000,40000,50000,60000,"
                                "70000,80000,90000,100000,90000,80000,70000,60000,50000,40000,30000,20000,30000,40000,50000";

    std::vector<uint64_t> m1Values = Util::splitWithStringDelimiter<uint64_t>(m1ValueString, ",");
    std::vector<uint64_t> m2Values = Util::splitWithStringDelimiter<uint64_t>(m2ValueString, ",");
    std::vector<uint64_t> d1Values = Util::splitWithStringDelimiter<uint64_t>(d1ValueString, ",");
    std::vector<uint64_t> d2Values = Util::splitWithStringDelimiter<uint64_t>(d2ValueString, ",");

    for (size_t i = 0; i < experimentRuntime; i++) {
        if (ingestionFrequencyDistribution == M1) {
            if (i >= m1Values.size()) break;
            predefinedIngestionRates.push_back(m1Values[i]);
        } else if (ingestionFrequencyDistribution == M2) {
            if (i >= m2Values.size()) break;
            predefinedIngestionRates.push_back(m2Values[i]);
        } else if (ingestionFrequencyDistribution == D1) {
            if (i >= d1Values.size()) break;
            predefinedIngestionRates.push_back(d1Values[i]);
        } else if (ingestionFrequencyDistribution == D2) {
            if (i >= d2Values.size()) break;
            predefinedIngestionRates.push_back(d2Values[i]);
        }
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
    sleep(5);   // TODO necessary?

    // TODO set predefinedIngestionRates for SINUS and COSINUS
    uint64_t buffersToProducePer10ms = 0;
    if (ingestionFrequencyDistribution != UNIFORM && ingestionFrequencyDistribution != SINUS && ingestionFrequencyDistribution != COSINUS) {
        buffersToProducePer10ms = predefinedIngestionRates[0];
    } else {
        buffersToProducePer10ms = ingestionRateInBuffers / 100;
    }

    NES_ASSERT(buffersToProducePer10ms != 0, "Ingestion ratio is too small");

    uint64_t nextPeriodStartTime = 0;
    uint64_t runningOverAllCount = 0;
    uint64_t productionCountPerInterval = 0;
    long lastSecond = 0;
    auto secondCount = 0;

    while (started) {
        auto periodStartTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        uint64_t buffersProcessedCount = 0;

        while (buffersProcessedCount < buffersToProducePer10ms) {
            auto bufferIndex = runningOverAllCount++ % preAllocatedBuffers.size();
            auto buffer = preAllocatedBuffers[bufferIndex];

            auto duration = std::chrono::high_resolution_clock::now().time_since_epoch();
            auto currentSecond = std::chrono::duration_cast<std::chrono::seconds>(duration).count();

            auto wrapBuffer = Runtime::TupleBuffer::wrapMemory(buffer.getBuffer(), buffer.getBufferSize(), this);
            wrapBuffer.setNumberOfTuples(buffer.getNumberOfTuples());
            wrapBuffer.setCreationTimestamp(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count());

            TupleBufferHolder bufferHolder;
            bufferHolder.bufferToHold = wrapBuffer;

            if (!bufferQueue.write(std::move(bufferHolder))) {
                NES_THROW_RUNTIME_ERROR("The queue is too small! This should not happen!");
            }

            productionCountPerInterval++;
            if (lastSecond != currentSecond) {
                lastSecond = currentSecond;

                if (ingestionFrequencyDistribution == SINUS) {
                    if (buffersToProducePer10ms == 0) buffersToProducePer10ms = 1;

                    //buffersToProducePer10ms = ;
                } else if (ingestionFrequencyDistribution == COSINUS) {
                    if (buffersToProducePer10ms == 0) buffersToProducePer10ms = 1;

                    //buffersToProducePer10ms = ;
                } else if (ingestionFrequencyDistribution == M1 || ingestionFrequencyDistribution == M2 ||
                           ingestionFrequencyDistribution == D1 || ingestionFrequencyDistribution == D2) {
                    buffersToProducePer10ms = predefinedIngestionRates[secondCount] / 100;
                } else {
                    NES_THROW_RUNTIME_ERROR("Mode of ingestionFrequencyDistribution not supported");
                }

                secondCount++;
            }// if (lastSecond != currentSecond)

            buffersProcessedCount++;
        }// while (buffersProcessedCount < buffersToProducePer10ms)
    }// while (started)
}
}// namespace NES::Benchmark::DateProviding
