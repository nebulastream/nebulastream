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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_NLJSLICE_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_NLJSLICE_HPP_

#include <Execution/Operators/Streaming/Join/StreamSlice.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSized.hpp>
#include <cstdint>
#include <mutex>
#include <ostream>
#include <span>
#include <vector>

struct Record {
    uint64_t id;
    uint64_t joinId;
    uint64_t value;
    uint64_t ingestionTimestamp;
    uint64_t processingTimestamp;
    uint64_t outputTimestamp;
};

namespace NES::Runtime::Execution {

using StreamSlicePtr = std::shared_ptr<StreamSlice>;

/**
 * @brief This class represents a single slice for the nested loop join. It stores all values for the left and right stream.
 * Later on this class can be reused for a slice.
 */
class NLJSlice : public StreamSlice {
  public:
    /**
     * @brief Constructor for creating a slice
     * @param sliceStart: Start timestamp of this slice
     * @param sliceEnd: End timestamp of this slice
     * @param numWorkerThreads: The number of worker threads that will operate on this slice
     * @param bufferManager: Allocates pages for both pagedVectorVarSized
     * @param leftSchema: schema of the tuple on the left
     * @param rightSchema: schema of the tuple on the right
     * @param leftPageSize: Size of a single page for the left paged vectors
     * @param rightPageSize: Size of a singe page for the right paged vectors
     */
    explicit NLJSlice(uint64_t sliceStart,
                      uint64_t sliceEnd,
                      uint64_t sliceId,
                      uint64_t numWorkerThreads,
                      BufferManagerPtr& bufferManager,
                      SchemaPtr& leftSchema,
                      uint64_t leftPageSize,
                      SchemaPtr& rightSchema,
                      uint64_t rightPageSize);

    ~NLJSlice() override = default;

    /**
     * @brief Retrieves the pointer to paged vector for the left or right side
     * @param workerThreadId: The id of the worker, which request the PagedVectorRef
     * @return Void pointer to the pagedVector
     */
    void* getPagedVectorRefLeft(WorkerThreadId workerThreadId);

    /**
     * @brief Retrieves the pointer to paged vector for the left or right side
     * @param workerThreadId: The id of the worker, which request the PagedVectorRef
     * @return Void pointer to the pagedVector
     */
    void* getPagedVectorRefRight(WorkerThreadId workerThreadId);

    /**
     * @brief combines the PagedVectors for the left and right side. Afterwards, all tuples are stored in the first
     * index of the vectors
     */
    void combinePagedVectors();

    /**
     * @brief Returns the number of tuples in this slice for the left side
     * @return uint64_t
     */
    uint64_t getNumberOfTuplesLeft() override;

    /**
     * @brief Returns the number of tuples in this slice for the right side
     * @return uint64_t
     */
    uint64_t getNumberOfTuplesRight() override;

    /**
     * @brief Creates a string representation of this slice
     * @return String
     */
    std::string toString() override;

    /**
     * @brief Gets left and right tuples' pages [Not implemented for variable sized pages right now]
     * Format of buffers looks like:
     * start metadata buffers contains metadata in format:
     * -----------------------------------------
     * number of metadata buffers | sliceId | sliceStart | sliceEnd | number of var sized pages (also number of workers) (n) | number of buffers in first var_sized_page (m_0) | ... | number of buffers in n-th var_sized_page (m_n)
     * uint64_t | uint64_t | uint64_t | uint64_t | ... | uint64_t
     * -----------------------------------------
     * all other buffers are: 1st buffer of 1st var_sized_page | .... | m_0 buffer of 1 var_sized_page | ... | 1 buffer of n-th var_sized_page | m_n buffer of n-th var_sized_page
     * @param bufferManager for getting new tuple buffers
     * @return list of pages that store records and metadata
     */
    std::vector<Runtime::TupleBuffer> serialize(BufferManagerPtr& bufferManager) override;

    /**
     * @brief Recreates the slice from tuple buffers [Not implemented for variable sized pages right now
     * Expected format:
     * start buffers contain metadata in format:
     * -----------------------------------------
     * number of metadata buffers | sliceId | sliceStart | sliceEnd | number of var sized pages (also number of workers) | number of buffers in first var_sized_page (m_0) | ... | number of buffers in n-th var_sized_page (m_n)
     * uint64_t | uint64_t | uint64_t | uint64_t | ... | uint64_t
     * -----------------------------------------
     * all other buffers are: 1st buffer of 1st var_sized_page | .... | m_0 buffer of 1 var_sized_page | ... | 1 buffer of n-th var_sized_page | m_n buffer of n-th var_sized_page
     * @param bufferManager: Allocates pages for both pagedVectorVarSized
     * @param leftSchema: schema of the tuple on the left
     * @param leftPageSize: Size of a single page for the left paged vectors
     * @param rightSchema: schema of the tuple on the right
     * @param rightPageSize: Size of a singe page for the right paged vectors
     * @param buffers List of tuple buffers to recreate the state of slice
     */
    static StreamSlicePtr deserialize(BufferManagerPtr& bufferManager,
                                      SchemaPtr& leftSchema,
                                      uint64_t leftPageSize,
                                      SchemaPtr& rightSchema,
                                      uint64_t rightPageSize,
                                      std::span<const Runtime::TupleBuffer> buffers);

  private:
    std::vector<std::unique_ptr<Nautilus::Interface::PagedVectorVarSized>> leftPagedVectors;
    std::vector<std::unique_ptr<Nautilus::Interface::PagedVectorVarSized>> rightPagedVectors;
};
}// namespace NES::Runtime::Execution

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_NLJSLICE_HPP_
