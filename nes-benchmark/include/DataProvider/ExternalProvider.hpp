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

namespace NES::Benchmark::DataProvision {

// TODO add support for workingTimeDelta, currently set to 10ms, it should be possible to set to e.g. 20ms
auto constexpr workingTimeDelta = 10;

class ExternalProvider : public DataProvider, public Runtime::BufferRecycler {
  public:
    /**
     * @brief creates an ExternalProvider
     * @param id
     * @param providerMode
     * @param preAllocatedBuffers
     * @param predefinedIngestionRates
     * @param ingestionRateCnt
     */
    ExternalProvider(uint64_t id,
                     DataProviderMode providerMode,
                     std::vector<Runtime::TupleBuffer> preAllocatedBuffers,
                     std::vector<uint64_t> predefinedIngestionRates,
                     uint64_t ingestionRateCnt);

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
     * @brief generates data based on predefinedIngestionRates
     */
    void generateData();

    std::vector<Runtime::TupleBuffer> preAllocatedBuffers;
    std::vector<uint64_t> predefinedIngestionRates;
    uint64_t ingestionRateCnt;
    folly::MPMCQueue<TupleBufferHolder> bufferQueue;
    bool started = false;
    std::thread generatorThread;
};
}// namespace NES::Benchmark::DataProvision

#endif//NES_EXTERNALPROVIDER_HPP
