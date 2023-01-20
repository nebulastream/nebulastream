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

// TODO turn M1,M2,D1,D2 into functions
// TODO use vector instead of array -> this may be resolved by the TODO above
constexpr uint64_t m1Values[] = {35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000,35000};
constexpr uint64_t m2Values[] = {10000,20000,30000,40000,50000,60000,70000,80000,90000,100000,90000,80000,70000,60000,50000,40000,30000,20000};
constexpr uint64_t d1Values[] = {10000,20000,30000,40000,50000,60000,70000,80000,90000,100000,90000,80000,70000,60000,50000,40000,30000,20000,30000,40000,50000,60000,70000,80000,90000,100000,90000,80000,70000,60000};
constexpr uint64_t d2Values[] = {100000,90000,80000,70000,60000,50000,40000,30000,20000,10000,20000,30000,40000,50000,60000,70000,80000,90000,100000,90000,80000,70000,60000,50000,40000,30000,20000,30000,40000,50000};

// TODO constexpr for workingTimeDelta, currently set to 10ms, it should be possible to set to e.g. 20ms

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
     * @brief overrides the stop function and clears the preAllocatedBuffers
     */
    void stop() override;

  private:
    /**
     * @brief checks whether the given ingestion frequency distribution is supported
     * @param providerMode
     * @return IngestionFrequencyDistribution
     */
    IngestionFrequencyDistribution getDistributionFromString(std::string ingestionDistribution);

    /**
     * @brief fills predefinedIngestionRates with values based on the selected ingestion frequency distribution
     */
    void generateIngestionRates();

    /**
     * @brief generate SINUS and COSINUS distributed values
     */
    void generateTrigonometricValues(uint64_t xValue);

    /**
     * @brief generates data based on the given ingestion frequency distribution and in some cases rate
     */
    void generateData();

    folly::MPMCQueue<TupleBufferHolder> bufferQueue;
    std::vector<Runtime::TupleBuffer> preAllocatedBuffers;
    uint64_t ingestionRateInBuffers;
    uint64_t experimentRuntime;
    IngestionFrequencyDistribution ingestionFrequencyDistribution;
    std::vector<uint64_t> predefinedIngestionRates;
    bool started = false;
    std::thread generatorThread;
};
}// namespace NES::Benchmark::DataProviding

#endif//NES_EXTERNALPROVIDER_HPP
