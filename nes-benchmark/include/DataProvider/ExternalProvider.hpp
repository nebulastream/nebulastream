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

#ifndef NES_BENCHMARK_INCLUDE_DATAPROVIDER_EXTERNALPROVIDER_HPP_
#define NES_BENCHMARK_INCLUDE_DATAPROVIDER_EXTERNALPROVIDER_HPP_

#include <DataProvider/DataProvider.hpp>
#include <IngestionRateGeneration/IngestionRateGenerator.hpp>

namespace NES::Benchmark::DataProvision {
/**
 * @brief sets the time period in milliseconds in which the predefined amount of buffers is ingested
 */
auto constexpr workingTimeDeltaInMillSeconds = 10;

class ExternalProvider;
using ExternalProviderPtr = std::shared_ptr<ExternalProvider>;

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
      * @param ingestionRateGenerator
      * @param throwException: If this is set to true, then exceptions are thrown instead of warnings. There is one exception thrown
      * if the buffer can not be written to the queue. Another one is thrown, if the data could not been generated fast enough
      */
    ExternalProvider(uint64_t id,
                     const DataProviderMode providerMode,
                     const std::vector<Runtime::TupleBuffer> preAllocatedBuffers,
                     IngestionRateGeneration::IngestionRateGeneratorPtr ingestionRateGenerator,
                     bool throwException = true);

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
     * @brief returns a reference to bufferQueue
     * @return bufferQueue
     */
    folly::MPMCQueue<TupleBufferHolder>& getBufferQueue();

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
     * @brief getter for checking if the external provider has started
     * @return true, if the external provider is up and running
     */
    bool isStarted() const;

    /**
     * @brief overrides readNextBuffer by providing the next buffer to be added to the caller
     * @param sourceId
     * @return either the next buffer in the queue or std::nullopt
     */
    std::optional<Runtime::TupleBuffer> readNextBuffer(uint64_t sourceId) override;

    /**
     * @brief overrides the recyclePooledBuffer interface. We have nothing to add in this class
     * @param buffer
     */
    void recyclePooledBuffer(Runtime::detail::MemorySegment* buffer) override;

    /**
     * @brief overrides the recycleUnpooledBuffer interface. We have nothing to add in this class
     * @param buffer
     */
    void recycleUnpooledBuffer(Runtime::detail::MemorySegment* buffer) override;

    /**
     * @brief waits until the external provider has started
     */
    void waitUntilStarted();

    /**
     * @brief sets new value for throwException
     * @param throwException
     */
    void setThrowException(bool throwException);

  private:
    /**
     * @brief generates data based on predefinedIngestionRates
     */
    void generateData();

    std::vector<Runtime::TupleBuffer> preAllocatedBuffers;
    IngestionRateGeneration::IngestionRateGeneratorPtr ingestionRateGenerator;
    folly::MPMCQueue<TupleBufferHolder> bufferQueue;
    std::atomic<bool> started = false;
    std::mutex mutexStartProvider;
    std::condition_variable cvStartProvider;
    std::thread generatorThread;
    std::vector<uint64_t> predefinedIngestionRates;
    bool throwException;
};
}// namespace NES::Benchmark::DataProvision

#endif // NES_BENCHMARK_INCLUDE_DATAPROVIDER_EXTERNALPROVIDER_HPP_
