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
#include <IngestionRateGeneration/IngestionRateGenerator.hpp>

namespace NES::Benchmark::DataProvision {
/**
 * @brief sets the time period in milliseconds in which the predefined amount of buffers is ingested
 */
auto constexpr workingTimeDelta = 10;

/**
 * @brief This class inherits from DataProvider. It enables the use of dynamic ingestion rates.
 */
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
                     IngestionRateGeneration::IngestionRateGeneratorPtr ingestionRateGenerator);

    /**
     * @brief destructor
     */
    ~ExternalProvider() override;

    /**
     * @brief returns a reference to preAllocatedBuffers
     * @return preAllocatedBuffers
     */
    std::vector<Runtime::TupleBuffer>& getPreAllocatedBuffers();

    /**
     * @brief returns a reference to generatorThread
     * @return generatorThread
     */
    std::thread& getGeneratorThread();

    /**
     * @brief overrides the start function and generates the data
     */
    void start() override;

    /**
     * @brief overrides the stop function and clears the preAllocatedBuffers
     */
    void stop() override;

    /**
     * @brief overrides readNextBuffer by providing the next buffer to be added to NES
     * @param sourceId
     * @return
     */
    std::optional<Runtime::TupleBuffer> readNextBuffer(uint64_t sourceId) override;

    /**
     * @brief overrides the recyclePooledBuffer interface. We have nothing to add in this class
     * @param buffer
     */
    void recyclePooledBuffer(Runtime::detail::MemorySegment *buffer) override;

    /**
     * @brief overrides the recycleUnpooledBuffer interface. We have nothing to add in this class
     * @param buffer
     */
    void recycleUnpooledBuffer(Runtime::detail::MemorySegment *buffer) override;

private:
    /**
     * @brief generates data based on predefinedIngestionRates
     */
    void generateData();

    std::vector<Runtime::TupleBuffer> preAllocatedBuffers;
    IngestionRateGeneration::IngestionRateGeneratorPtr ingestionRateGenerator;
    folly::MPMCQueue<TupleBufferHolder> bufferQueue;
    bool started = false;
    std::thread generatorThread;
};
}// namespace NES::Benchmark::DataProvision

#endif//NES_EXTERNALPROVIDER_HPP
