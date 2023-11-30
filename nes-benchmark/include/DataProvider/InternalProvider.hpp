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

#ifndef NES_BENCHMARK_INCLUDE_DATAPROVIDER_INTERNALPROVIDER_HPP_
#define NES_BENCHMARK_INCLUDE_DATAPROVIDER_INTERNALPROVIDER_HPP_

#include <DataProvider/DataProvider.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Benchmark::DataProvision {
/**
 * @brief This class inherits from DataProvider. The internal provider ingests the generated data as quickly as possible.
 */
class InternalProvider : public DataProvider, public Runtime::BufferRecycler {

  public:
    /**
     * @brief creates an InternalProvider
     * @param id
     * @param providerMode
     * @param preAllocatedBuffers
     */
    InternalProvider(uint64_t id, DataProvider::DataProviderMode providerMode, std::vector<Runtime::TupleBuffer> buffers);

    /**
     * @brief destructor
     */
    ~InternalProvider() override;

    /**
     * @brief returns a reference to preAllocatedBuffers
     * @return preAllocatedBuffers
     */
    std::vector<Runtime::TupleBuffer>& getPreAllocatedBuffers();

    /**
     * @brief overrides readNextBuffer by providing the next buffer to be added to the caller
     * @param sourceId
     * @return either the next buffer in line or std::nullopt
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
     * @brief overrides the start function. We have nothing to add in this class
     */
    void start() override;

    /**
     * @brief overrides the stop function, we clear all the preAllocatedBuffers
     */
    void stop() override;

  private:
    std::vector<Runtime::TupleBuffer> preAllocatedBuffers;
    uint64_t currentlyEmittedBuffer = 0;
    bool started = false;
};
}// namespace NES::Benchmark::DataProvision

#endif  // NES_BENCHMARK_INCLUDE_DATAPROVIDER_INTERNALPROVIDER_HPP_
