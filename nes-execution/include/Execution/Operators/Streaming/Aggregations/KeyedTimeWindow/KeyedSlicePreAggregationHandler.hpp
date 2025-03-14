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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_KEYEDTIMEWINDOW_KEYEDSLICEPREAGGREGATIONHANDLER_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_KEYEDTIMEWINDOW_KEYEDSLICEPREAGGREGATIONHANDLER_HPP_

#include <Execution/Operators/Streaming/Aggregations/AbstractSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <vector>

namespace NES::Runtime::Execution::Operators {

class MultiOriginWatermarkProcessor;
class KeyedThreadLocalSliceStore;
class State;

class KeyedSlicePreAggregationHandler;
using KeyedSlicePreAggregationHandlerPtr = std::shared_ptr<KeyedSlicePreAggregationHandler>;
/**
 * @brief The KeyedSlicePreAggregationHandler provides an operator handler to perform slice-based pre-aggregation of keyed tumbling windows.
 * @note sliding windows will be added later.
 * This operator handler, maintains a slice store for each worker thread and provides them for the aggregation.
 * For each processed tuple buffer triggerThreadLocalState is called, which checks if the thread-local slice store should be triggered.
 * This is decided by the current watermark timestamp.
 */
class KeyedSlicePreAggregationHandler : public AbstractSlicePreAggregationHandler<KeyedSlice, KeyedThreadLocalSliceStore> {
  public:
    /**
     * @brief Creates the operator handler with a specific window definition, a set of origins, and access to the slice staging object.
     * @param windowDefinition logical window definition
     * @param origins the set of origins, which can produce data for the window operator
     * @param weakSliceStagingPtr access to the slice staging.
     */
    KeyedSlicePreAggregationHandler(uint64_t windowSize, uint64_t windowSlide, const std::vector<OriginId>& origins);

    /**
     * @brief Initializes the thread local state for the window operator
     * @param ctx PipelineExecutionContext
     * @param entrySize Size of the aggregated values in memory
     */
    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t keySize, uint64_t valueSize);

    void recreate() override;

    bool shouldRecreate() override;

    /**
     * @brief Recreate state and window info from the file
     */
    void recreateOperatorHandlerFromFile();

    /**
      * @brief Retrieves state, window info and watermarks information from max(last watermark, migrated tmstmp)
      * @return vector of tuple buffers
      */
    std::vector<Runtime::TupleBuffer> serializeOperatorHandlerForMigration() override;

    std::vector<Runtime::TupleBuffer> getSerializedPortion(uint64_t id) override;

    /**
     * @brief Retrieve the state as a vector of tuple buffers
     * Format of buffers looks like:
     * start buffers contain metadata in format:
     * -----------------------------------------
     * number of metadata buffers | number of slices (n) | number of buffers in 1st slice (m_0) | ... | number of slices in n-th slice (m_n)
     * uint64_t | uint64_t | uint64_t | ... | uint64_t
     * -----------------------------------------
     * all other buffers are: 1st buffer of 1st slice | .... | m_0 buffer of 1 slice | ... | 1 buffer of n-th slice | m_n buffer of n-th slice
     * @param startTS as a left border of slices
     * @param stopTS as a right border of slice
     * @return vector of tuple buffers
     */
    std::vector<Runtime::TupleBuffer> getStateToMigrate(uint64_t startTS, uint64_t stopTS) override;

    /**
     * @brief Restores the state from vector of tuple buffers
     * Expected format of buffers:
     * start buffers contain metadata in format:
     * -----------------------------------------
     * number of slices (n) | number of buffers in 1st slice (m_0) | ... | number of slices in n-th slice (m_n)
     * uint64_t | uint64_t | uint64_t | ... | uint64_t
     *-----------------------------------------
     * all other buffers are: 1st buffer of 1st slice | .... | m_0 buffer of 1 slice | ... | 1 buffer of n-th slice | m_n buffer of n-th slice
     * @param buffers with the state
     */
    void restoreState(std::vector<Runtime::TupleBuffer>& buffers) override;

    /**
    * @brief read numberOfBuffers amount of buffers from file
    * @param numberOfBuffers number of buffers to read
    * @return read buffers
    */
    std::vector<TupleBuffer> readBuffers(std::ifstream& stream, uint64_t numberOfBuffers);

    /**
     * @brief Retrieve watermarks and sequence numbers as a vector of tuple buffers
     * Format of buffers looks like:
     * buffers contain metadata in format:
     * -----------------------------------------
     * number of build origins (n) | number of probe origins (m)
     * | watermark of 1st origin (i_0) | ... | watermark of n-th slice (i_n) | seq number of 1st origin (i_0) | ... | seq number of m-th slice (i_m)
     * uint64_t | uint64_t | uint64_t | ... | uint64_t
     */
    std::vector<TupleBuffer> getWatermarksToMigrate();

    /**
     * @brief Restores watermarks and sequence numbers in build and probe watermark processors
     * Format of buffers looks like:
     * buffers contain metadata in format:
     * -----------------------------------------
     * number of build buffers (n) | number of probe buffers (m)
     * watermark of 1st origin (i_0) | ... | watermark of n-th slice (i_n) | seq number of 1st origin (i_0) | ... | seq number of m-th slice (i_m)
     * uint64_t | uint64_t | uint64_t | ... | uint64_t
     */
    void restoreWatermarks(std::vector<Runtime::TupleBuffer>& buffers);

    void setBufferManager(const BufferManagerPtr& bufManager);

    /**
     * set file with the state and recreation flag
     */
    void setRecreationFileName(std::string filePath);
    /**
     * get recreation file name
     */
    std::optional<std::string> getRecreationFileName();

    ~KeyedSlicePreAggregationHandler() override;

    uint64_t getCurrentWatermark() const;

    bool migrating = false;

    private:
    /**
     * Deserialize slice from span of buffers
     * @param buffers as a span
     * @return recreated KeyedSlicePtr
     */
    KeyedSlicePtr deserializeSlice(std::span<const Runtime::TupleBuffer> buffers);
    std::atomic<bool> shouldBeRecreated{false};
    std::optional<std::string> recreationFilePath;
        std::vector<TupleBuffer> stateToTransfer{};
    std::vector<bool> asked{false, false, false, false};

  protected:
    BufferManagerPtr bufferManager;
};
}// namespace NES::Runtime::Execution::Operators
#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_KEYEDTIMEWINDOW_KEYEDSLICEPREAGGREGATIONHANDLER_HPP_
