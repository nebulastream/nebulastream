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

#ifndef NES_EXTERNALPROVIDER_HPP
#define NES_EXTERNALPROVIDER_HPP

#include <DataProvider/DataProvider.hpp>

namespace NES::Benchmark::DataProviding {

enum IngestionFrequencyDistribution {
    UNIFORM,
    SINUS,
    COSINUS,
    M1,
    M2,
    D1,
    D2
};

class ExternalProvider : public DataProvider, public Runtime::BufferRecycler {
  public:
    /**
     * @brief creates an ExternalProvider
     * @param id
     * @param preAllocatedBuffers
     * @param ingestionRateInBuffers only necessary if ingestionFrequencyDistribution is set to "UNIFORM" or "SINUS" or "COSINUS"
     * @param experimentRuntime
     * @param providerMode
     * @param ingestionFrequencyDistribution
     */
    ExternalProvider(uint64_t id,
                     std::vector<Runtime::TupleBuffer> preAllocatedBuffers,
                     uint64_t ingestionRateInBuffers,
                     uint64_t experimentRuntime,
                     DataProviderMode providerMode,
                     std::string ingestionFrequencyDistribution);

    /**
     * @brief overrides the start function and generates the data
     */
    void start() override;

    /**
     * @brief overrides the stop function
     */
    void stop() override;

  private:
    IngestionFrequencyDistribution getModeFromString(std::string providerMode);
    void generateFrequencies(uint64_t experimentRuntime);
    void generateData();

    folly::MPMCQueue<TupleBufferHolder> bufferQueue;
    std::vector<Runtime::TupleBuffer> preAllocatedBuffers;
    uint64_t ingestionRateInBuffers;
    //uint64_t experimentRuntime;
    IngestionFrequencyDistribution ingestionFrequencyDistribution;
    std::vector<uint64_t> predefinedIngestionRates;
    bool started = false;
    std::thread generatorThread;
};
}// namespace NES::Benchmark::DataProviding

#endif//NES_EXTERNALPROVIDER_HPP
