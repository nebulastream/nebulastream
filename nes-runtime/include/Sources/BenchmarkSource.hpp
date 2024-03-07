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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_BENCHMARKSOURCE_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_BENCHMARKSOURCE_HPP_

#include <Runtime/BufferRecycler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/GeneratorSource.hpp>
#include <Util/GatheringMode.hpp>
#include <Util/SourceMode.hpp>

namespace NES {
namespace detail::Runtime {
class MemorySegment;
}// namespace detail::Runtime

/**
 * @brief Benchmark Source is a special source for benchmarking purposes only and stripes away all overhead
 * The memory area out of which buffers will be produced must be initialized beforehand and allocated as a shared_ptr
 * that must have ownership of the area, i.e., it must control when to free it.
 * Do not use in distributed settings but only for single node dev and testing.
 */
class BenchmarkSource : public GeneratorSource, public Runtime::BufferRecycler {
  public:
    /**
     * @brief The constructor of a BenchmarkSource
     * @param schema the schema of the source
     * @param memoryArea the non-null memory area that stores the data that will be used by the source
     * @param memoryAreaSize the non-zero size of the memory area
     * @param bufferManager pointer to the buffer manager
     * @param queryManager pointer to the query manager
     * @param numberOfBuffersToProcess the number of buffers to be produced by the source
     * @param gatheringValue how many tuples to collect per interval
     * @param operatorId current operator id
     * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
     * @param statisticId represents the unique identifier of components that we can track statistics for
     * @param numSourceLocalBuffers the number of buffers allocated to a source
     * @param gatheringMode the gathering mode (INTERVAL_MODE, INGESTION_RATE_MODE, or ADAPTIVE_MODE)
     * @param sourceMode
     * @param sourceAffinity the subsequent operators in the pipeline to which the data is pushed
     * @param taskQueueId the ID of the queue to which the task is pushed
     * @param physicalSourceName the name and unique identifier of a physical source
     * @param successors the subsequent operators in the pipeline to which the data is pushed
     * @return a DataSourcePtr pointing to the data source
     */
    explicit BenchmarkSource(SchemaPtr schema,
                             const std::shared_ptr<uint8_t>& memoryArea,
                             size_t memoryAreaSize,
                             Runtime::BufferManagerPtr bufferManager,
                             Runtime::QueryManagerPtr queryManager,
                             uint64_t numberOfBuffersToProcess,
                             uint64_t gatheringValue,
                             OperatorId operatorId,
                             OriginId originId,
                             StatisticId statisticId,
                             size_t numSourceLocalBuffers,
                             GatheringMode gatheringMode,
                             SourceMode sourceMode,
                             uint64_t sourceAffinity,
                             uint64_t taskQueueId,
                             const std::string& physicalSourceName,
                             std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);

    ~BenchmarkSource();

    /**
     * @brief This method is implemented only to comply with the API: it will crash the system if called.
     * @return a nullopt
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief Provides a string representation of the source
     * @return The string representation of the source
     */
    std::string toString() const override;

    /**
     * @brief Provides the type of the source
     * @return the type of the source
     */
    SourceType getType() const override;

    /**
     * @brief running routine while source is active,
     */
    virtual void runningRoutine() override;

    virtual void recyclePooledBuffer(Runtime::detail::MemorySegment*) override;

    /**
     * @brief Interface method for unpooled buffer recycling
     * @param buffer the buffer to recycle
     */
    virtual void recycleUnpooledBuffer(Runtime::detail::MemorySegment*) override;

    /**
     * @brief This methods creates the local buffer pool and is necessary because we cannot do it in the constructor
     */
    void open() override;

    void close() override;

  private:
    uint64_t numberOfTuplesToProduce;
    std::shared_ptr<uint8_t> memoryArea;
    const size_t memoryAreaSize;
    Runtime::TupleBuffer numaLocalMemoryArea;
    uint64_t currentPositionInBytes;
    SourceMode sourceMode;
    uint64_t schemaSize;
    uint64_t bufferSize;
    std::atomic<uint64_t> memoryAreaRefCnt{0};
};

using BenchmarkSourcePtr = std::shared_ptr<BenchmarkSource>;

}// namespace NES

#endif// NES_RUNTIME_INCLUDE_SOURCES_BENCHMARKSOURCE_HPP_
